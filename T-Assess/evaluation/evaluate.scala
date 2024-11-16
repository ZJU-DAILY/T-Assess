package evaluation

import java.util.Date
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.util.stream.Collectors
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer
import validity._
import completeness._
import consistency._
import kit._

object evaluate {
  private def offlineEval(sc: SparkContext, items: Map[String, String]): Unit = {
    printf("%s *** processing %s\n", util.timestamp2string(new Date().getTime), items("dataset"))
    var trajRDD = sc.wholeTextFiles(items("dataset"), minPartitions = variables.num_partition).map(_._2.split("\n").map(_.split(",")).map(x => (GPS(x(1).toDouble, x(2).toDouble), Triplet(x(0), x(3).toInt, x(4).toLong)))) // x(1)-longitude x(2)-latitude)
      .map(_.iterator.toIterable)
      //.sample(withReplacement = false, fraction = 0.01, seed = 1)
    if (items("sample") != "") {
      val ids = Files.lines(Paths.get(items("sample"))).skip(5).collect(Collectors.toList())
      trajRDD = trajRDD.filter(x => ids.contains(x.head._2.tid.toString))
    }
    trajRDD.persist(StorageLevel.MEMORY_ONLY)
    val pointRDD = trajRDD.flatMap(x => x).persist(StorageLevel.MEMORY_ONLY)

//    var recordRDD = sc.textFile(items("dataset"), minPartitions = variables.num_partition).map(_.split(","))
//      .map(x => (GPS(x(1).toDouble, x(2).toDouble), Triplet(x(0), x(3).toInt, x(4).toLong))) // x(1)-longitude x(2)-latitude
//      .persist(StorageLevel.MEMORY_ONLY)
//    if (items("sample") != "") {
//      val ids = Files.lines(Paths.get(items("sample"))).skip(5).collect(Collectors.toList())
//      recordRDD = recordRDD.filter(x => ids.contains(x._2.tid.toString))
//    }
//    val trajRDD = recordRDD/*.coalesce(1)*/
//      .groupBy(_._2.tid).map(_._2)
//      //.repartition(variables.num_partition)
//      //.sample(withReplacement = false, fraction = 0.01, seed = 1)
//      .persist(StorageLevel.MEMORY_ONLY)

    println(util.timestamp2string(new Date().getTime) + " *** Statistics")
    variables.t_num = trajRDD.count().toInt
    variables.p_num = pointRDD.count()
    printf(" *** %d trajectories\n", variables.t_num)
    printf(" *** %d points\n", variables.p_num)

    val validity_start_time = new Date().getTime
    printf("%s *** Evaluate with isInRange\n", util.timestamp2string(validity_start_time))
    val isInRange_result = trajRDD.filter(isInRange.eval).map(_.head._2.tid)
    variables.isInRange_tids.addAll(isInRange_result.collect())
    printf(" *** %d trajectories violate isInRange\n", variables.isInRange_tids.size)

    printf("%s *** Evaluate with isOrderConstraint\n", util.timestamp2string(new Date().getTime))
    val isOrderConstraint_result = trajRDD.filter(isOrderConstraint.eval).map(_.head._2.tid)
    variables.isOrderConstraint_tids.addAll(isOrderConstraint_result.collect())
    printf(" *** %d trajectories violate isOrderConstraint\n", variables.isOrderConstraint_tids.size)
    printf("Elapsed time for validity: %.3fs\n", (new Date().getTime - validity_start_time) / 1000.0)

    val completeness_start_time = new Date().getTime
    printf("%s *** Evaluate with hasMP\n", util.timestamp2string(completeness_start_time))
    val items_broadcast = sc.broadcast(items)
    val hasMP_result = trajRDD.map(x => (x.head._2.tid, hasMP.eval(x, items_broadcast.value("mode") != "stream")))
      .map(y => (y._1, y._2._1))
      .filter(_._2 != 0)
      .persist(StorageLevel.MEMORY_ONLY)
    variables.hasMP_tids.addAll(hasMP_result.map(_._1).collect())
    variables.hasMP_pcnt = hasMP_result.map(_._2).sum().toLong
    printf(" *** %d trajectories violate hasMP\n", variables.hasMP_tids.size)
    printf("Elapsed time for completeness: %.3fs\n", (new Date().getTime - completeness_start_time) / 1000.0)

    val consistency_start_time = new Date().getTime
    printf("%s *** Evaluate with isTLC\n", util.timestamp2string(consistency_start_time))
    val traj_len = trajRDD.map(x => (x.head._2.tid, isTLC.eval(x))).collect()
    val len_cal = traj_len.map(_._2)
    val mean = len_cal.sum / len_cal.length
    val std = math.sqrt(len_cal.map(x => math.pow(x - mean, 2)).sum / len_cal.length)
    printf(" *** length mean - %f, std - %f\n", mean, std)
    val length_thresh = math.max(0, mean - 3 * std)
    val isTLC_result = traj_len.filter(x => x._2 < length_thresh).map(_._1)
    variables.isTLC_tids.addAll(isTLC_result)
    printf(" *** %d trajectories violate isTLC\n", variables.isTLC_tids.size)

    if (items("bfmap") != "") {
      printf("%s *** Evaluate with isRN\n", util.timestamp2string(new Date().getTime))
      // TODO: 集群情况下有些节点的matcher为null
      val matcher_broadcast = sc.broadcast(variables.map_matcher)
      val isRN_result = trajRDD.map(x => (x.head._2.tid, isRN.evalOff(x, matcher_broadcast.value))).filter(_._2.nonEmpty).persist(StorageLevel.MEMORY_ONLY)
      variables.isRN_tids.addAll(isRN_result.map(_._1).collect())
      variables.isRN_pids.addAll(isRN_result.flatMap(_._2).collect())
      printf(" *** %d trajectories violate isRN\n", variables.isRN_tids.size)
      //printf(" *** %d points violate isRN\n", variables.isRN_pids.size)
    }

    printf("%s *** Evaluate with isSC\n", util.timestamp2string(new Date().getTime))
    val slow_point_thresh_broadcast = sc.broadcast(variables.slow_point_thresh)
    val min_time_broadcast = sc.broadcast(variables.min_time)
    val eps_broadcast = sc.broadcast(variables.eps)
    val ps = trajRDD.map(isSC.trajDBSCAN(_, slow_point_thresh_broadcast.value, min_time_broadcast.value, eps_broadcast.value)).flatMap(x => x).collect()
    val isSC_result = isSC.clusterPS(ps, variables.cluster_eps).filter(x => x.map(_._1).distinct.length == 1)
    variables.isSC_tids.addAll(isSC_result.flatMap(x => x.map(y => y._1)))
    variables.isSC_pids.addAll(isSC_result.flatMap(x => x.map(y => y._2)))
    printf(" *** %d trajectories violate isSC\n", variables.isSC_tids.size)
    //printf(" *** %d points violate isSC\n", variables.isSC_pids.size)

    printf("%s *** Evaluate with isPSM\n", util.timestamp2string(new Date().getTime))
    val σ_broadcast = sc.broadcast(variables.σ)
    val σ_s_broadcast = sc.broadcast(variables.σ_s)
    val deviation_broadcast = sc.broadcast(variables.deviation)
    val isPSM_result = trajRDD.map(x => (x.head._2.tid, isPSM.kalmanFilter(x, σ_broadcast.value, σ_s_broadcast.value, deviation_broadcast.value))).filter(_._2.nonEmpty).persist(StorageLevel.MEMORY_ONLY)
    variables.isPSM_tids.addAll(isPSM_result.map(_._1).collect())
    variables.isPSM_pids.addAll(isPSM_result.flatMap(_._2).collect())
    printf(" *** %d trajectories violate isPSM\n", variables.isPSM_tids.size)
    //printf(" *** %d points violate isPSM\n", variables.isPSM_pids.size)

    // TODO: 若需要warn的信息，修改/home/wt/spark/conf/log4j2.properties的日志等级
    printf("%s *** Evaluate with isTO\n", util.timestamp2string(new Date().getTime))
    val geo_bound_broadcast = sc.broadcast(variables.geo_bound)
    val traj_with_coarse_segment = trajRDD.map(x => (x.head._2.tid, x.map(y => isTO.grid(y._1, geo_bound_broadcast.value))))
      .map(z => (z._1, isTO.createCoarseSegments(z._2.toArray, z._1)))
      .persist(StorageLevel.MEMORY_ONLY)
    val traj_distance = traj_with_coarse_segment.map(x => (x._1, x._2.map(_.len).sum)).collect().toMap
    val coarse_segments = traj_with_coarse_segment.flatMap(_._2).zipWithIndex()
      // TODO: 可替代的加速方法
      .sample(withReplacement = false, fraction = 0.1, seed = 1)
      .coalesce(math.sqrt(variables.num_partition).toInt)
      .persist(StorageLevel.MEMORY_ONLY)
    coarse_segments.count()
    val pairs = coarse_segments.cartesian(coarse_segments).filter(x => x._1._1.tid != x._2._1.tid && x._1._2 < x._2._2).map(y => (y._1._1,y._2._1 ))
    val D_broadcast = sc.broadcast(variables.D)
    val w_per_broadcast = sc.broadcast(variables.w_per)
    val w_pral_broadcast = sc.broadcast(variables.w_pral)
    val w_ang_broadcast = sc.broadcast(variables.w_ang)
    val CLS = pairs.map(x => isTO.calculateCL(x._1, x._2, D_broadcast.value, w_per_broadcast.value, w_pral_broadcast.value, w_ang_broadcast.value))
      .flatMap(y => y).reduceByKey(_ ++ _)
    val F_broadcast = sc.broadcast(variables.F)
    val t_num_broadcast = sc.broadcast(variables.t_num)
    val P_broadcast = sc.broadcast(variables.P)
    val isTO_result = CLS.map(x => ((x._1.len, x._1.tid), x._2))
      .filter(y => isTO.fineSegmentOutlier(y._1._1, y._2, t_num_broadcast.value, P_broadcast.value))
      .map(z => (z._1._2, z._1._1))
      .reduceByKey(_ + _).filter(i => i._2/ traj_distance(i._1) >= F_broadcast.value)
      .map(_._1)
    variables.isTO_tids.addAll(isTO_result.collect())
    printf(" *** %d trajectories violate isTO\n", variables.isTO_tids.size)
    printf("Elapsed time for consistency: %.3fs\n", (new Date().getTime - consistency_start_time) / 1000.0)

//    val fairness_start_time = new Date().getTime
//    printf("%s *** Construct Index\n", util.timestamp2string(new Date().getTime))
//    val cap_broadcast = sc.broadcast(variables.cap)
//    val quad_trees = pointRDD.mapPartitions(iter => {
//      val qt = new QuadTree(cap_broadcast.value, geo_bound_broadcast.value)
//      qt.build(iter.to(Iterable))
//      Iterator(qt)
//    }).persist(StorageLevel.MEMORY_ONLY)
//    val GMTs = pointRDD.map(x => util.string2timestamp(x._2.GMT))
//    variables.time_bound = TimeSpan(GMTs.min(), GMTs.max)
//    val order_broadcast = sc.broadcast(variables.order)
//    val b_plus_trees = quad_trees.map(_.dfsBPlusTree(order_broadcast.value)).persist(StorageLevel.MEMORY_ONLY)
//    b_plus_trees.count()
//    // TODO: 确定是否还需要使用线段树
//    //val segment_trees = b_plus_trees.map(x => new SegmentTree(x., x)).persist(StorageLevel.MEMORY_ONLY)
//
//    printf("%s *** Evaluate with isSpatialUniform\n", util.timestamp2string(fairness_start_time))
//    var isSpatialUniform_result = Array[(Box, Double)]()
//    for (box <- variables.boxs) {
//      val cnt = quad_trees.map(_.rangeSearch(box).length.toDouble).sum()
//      val i = (cnt / variables.p_num) * (variables.geo_bound.getArea / box.getArea)
//      val density = i / (i + 1)
//      isSpatialUniform_result :+= (box, density)
//    }
//    variables.isSpatialUniform_density.addAll(isSpatialUniform_result)
//
//    printf("%s *** Evaluate with isTemporalUniform\n", util.timestamp2string(new Date().getTime))
//    var isTemporalUniform_result = Array[(TimeSpan, Double)]()
//    for (span <- variables.timespan) {
//      //val related =  segment_trees.map(x => (x._1.query(span.start / 1000, span.end / 1000), x._2))
//      //val cnt = related.map(x => x._1.flatMap(y => x._2(y)._1.boundedKeysIterator((Symbol(">="), (span.start, -1)), (Symbol("<"), (span.end + 1, -1)))).size.toDouble).sum()
//      val related =  b_plus_trees.flatMap(x => x).filter(_._2.overlap(span)).map(_._1)
//      val cnt = related.flatMap(x => x.boundedKeysIterator((Symbol(">="), (span.start, -1)), (Symbol("<"), (span.end + 1, -1)))).count().toDouble
//      val i = (cnt / variables.p_num) * (variables.time_bound.getInterval.toDouble / span.getInterval.toDouble)
//      val density = i / (i + 1)
//      isTemporalUniform_result :+= (span, density)
//    }
//    variables.isTemporalUniform_density.addAll(isTemporalUniform_result)
//    printf("Elapsed time for fairness: %.3fs\n", (new Date().getTime - fairness_start_time) / 1000.0)

    printf("%s *** done\n", util.timestamp2string(new Date().getTime))
  }

  private def onlineEval(ssc: StreamingContext, items: Map[String, String]): Unit = {
    val topics = Array("trajSender")
    val kafka_params = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "traQA"
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      Subscribe[String, String](topics, kafka_params)
    )

    val inputStream = stream.map(_.value().split(","))
    inputStream.foreachRDD(RDD => {
      val start_time = new Date().getTime
      printf("------------------------------------------------------------\nTime: %d ms\n------------------------------------------------------------\n", start_time)

      // TODO: 分布式环境能访问kafka broker
      if (!RDD.isEmpty()) {
        variables.window_point_cnt :+= RDD.count()
        printf(" *** %d points ***\n", variables.window_point_cnt.last)

        val recordRDD = RDD.repartition(variables.num_partition)
          .map(x => (GPS(x(1).toDouble, x(2).toDouble), Triplet(x(0), x(3).toInt, x(4).toLong)))
          .persist(StorageLevel.MEMORY_ONLY)
        val trajRDD = recordRDD.coalesce(1)
          .groupBy(_._2.tid).map(_._2.toSeq.sortBy(_._2.pid))
          .repartition(variables.num_partition)
          .persist(StorageLevel.MEMORY_ONLY)
        trajRDD.count()

        val validity_start_time = new Date().getTime
        printf("%s *** Evaluate with isInRange\n", util.timestamp2string(validity_start_time))
        val isInRange_result = trajRDD.filter(isInRange.eval).map(_.head._2.tid)
        variables.isInRange_tids.addAll(isInRange_result.collect())

        printf("%s *** Evaluate with isOrderConstraint\n", util.timestamp2string(new Date().getTime))
        val isOrderConstraint_result = trajRDD.filter(isOrderConstraint.eval).map(_.head._2.tid)
        variables.isOrderConstraint_tids.addAll(isOrderConstraint_result.collect())
        variables.validity_cost :+= (new Date().getTime - validity_start_time) / 1000.0

        val completeness_start_time = new Date().getTime
        printf("%s *** Evaluate with hasMP\n", util.timestamp2string(completeness_start_time))
        val items_broadcast = ssc.sparkContext.broadcast(items)
        val heaps = variables.heaps
        val hasMP_result = trajRDD.map(x => (x.head._2.tid, hasMP.eval(x, items_broadcast.value("mode") != "stream", if (heaps(x.head._2.tid).isEmpty) 0 else heaps(x.head._2.tid).head._2))).persist(StorageLevel.MEMORY_ONLY)
        val intervals = hasMP_result.map(x => (x._1, x._2._2)).collect()
        for (pair <- intervals) {
          for (time <- pair._2) {
            if (!variables.heaps(pair._1).exists(_._2 == time)) {
              variables.heaps(pair._1).enqueue((1, time))
            } else {
              val obj = variables.heaps(pair._1).filter(_._2 == time).head
              variables.heaps(pair._1) = variables.heaps(pair._1).filter(_._2 != time)
              variables.heaps(pair._1).enqueue((obj._1 + 1, obj._2))
            }
          }
        }
        variables.hasMP_tids.addAll(hasMP_result.filter(_._2._1 != 0).map(_._1).collect())
        variables.hasMP_pcnt += hasMP_result.map(x => x._2._1).sum().toLong
        variables.completeness_cost :+= (new Date().getTime - completeness_start_time) / 1000.0

        val consistency_start_time = new Date().getTime
        printf("%s *** Evaluate with isTLC\n", util.timestamp2string(consistency_start_time))
        val isTLC_result = trajRDD.map(x => (x.head._2.tid, isTLC.eval(x)))
        for (pair <- isTLC_result.collect()) {
          variables.lens(pair._1) = variables.lens(pair._1) + pair._2
        }

        if (items("bfmap") != "") {
          printf("%s *** Evaluate with isRN\n", util.timestamp2string(new Date().getTime))
          val matcher_broadcast = ssc.sparkContext.broadcast(variables.map_matcher)
          val matcherKStates = variables.matcherKStates
          val isRN_result = trajRDD.map(x => (x.head._2.tid, isRN.evalOn(x, matcherKStates(x.head._2.tid), matcher_broadcast.value))).persist(StorageLevel.MEMORY_ONLY)
          for (pair <- isRN_result.map(x => (x._1, x._2._2)).collect()) {
            variables.matcherKStates(pair._1) = pair._2
          }
          variables.isRN_tids.addAll(isRN_result.filter(_._2._1.length > 0).map(_._1).collect())
          variables.isRN_pids.addAll(isRN_result.flatMap(_._2._1).collect())
        }

        printf("%s *** Evaluate with isPSM\n", util.timestamp2string(new Date().getTime))
        val σ_broadcast = ssc.sparkContext.broadcast(variables.σ)
        val σ_s_broadcast = ssc.sparkContext.broadcast(variables.σ_s)
        val deviation_broadcast = ssc.sparkContext.broadcast(variables.deviation)
        val isPSM_result = trajRDD.map(x => (x.head._2.tid, isPSM.kalmanFilter(x, σ_broadcast.value, σ_s_broadcast.value, deviation_broadcast.value))).filter(_._2.nonEmpty).persist(StorageLevel.MEMORY_ONLY)
        variables.isPSM_tids.addAll(isPSM_result.map(_._1).collect())
        variables.isPSM_pids.addAll(isPSM_result.flatMap(_._2).collect())

        printf("%s *** Evaluate with isSC\n", util.timestamp2string(new Date().getTime))
        val stop_groups = variables.stop_groups
        val eps_broadcast = ssc.sparkContext.broadcast(variables.eps)
        val min_time_broadcast = ssc.sparkContext.broadcast(variables.min_time)
        val ps_and_group = trajRDD.map(x => (x.head._2.tid, isSC.geoStopDetection(x.toArray, stop_groups(x.head._2.tid), eps_broadcast.value, min_time_broadcast.value))).persist(StorageLevel.MEMORY_ONLY)
        for (i <- ps_and_group.map(x => (x._1, x._2._2)).collect()) {
          variables.stop_groups.put(i._1, i._2)
        }
        variables.ps :++= ps_and_group.flatMap(x => x._2._1).collect()

        printf("%s *** Evaluate with isTO\n", util.timestamp2string(new Date().getTime))
        val geo_bound_broadcast = ssc.sparkContext.broadcast(variables.geo_bound)
        val coarse_segments = trajRDD.map(x => (x.head._2.tid, x.map(y => isTO.grid(y._1, geo_bound_broadcast.value))))
          .flatMap(z => isTO.createCoarseSegments(z._2.toArray, z._1)).zipWithIndex()
          //.sample(withReplacement = false, fraction = 0.1, seed = 1)
          .coalesce(math.sqrt(variables.num_partition).toInt)
          .persist(StorageLevel.MEMORY_ONLY)
        coarse_segments.count()
        val pairs = coarse_segments.cartesian(coarse_segments)
          .filter(x => x._1._1.tid != x._2._1.tid && x._1._2 < x._2._2).map(y => (y._1._1,y._2._1 ))
        val D_broadcast = ssc.sparkContext.broadcast(variables.D)
        val w_per_broadcast = ssc.sparkContext.broadcast(variables.w_per)
        val w_pral_broadcast = ssc.sparkContext.broadcast(variables.w_pral)
        val w_ang_broadcast = ssc.sparkContext.broadcast(variables.w_ang)
        val t_num_broadcast = ssc.sparkContext.broadcast(variables.t_num)
        val isTO_result = pairs.filter(x => isTO.dist(x._1.pointList.head, x._1.pointList.last, x._2.pointList.head, x._2.pointList.last, w_per_broadcast.value, w_pral_broadcast.value, w_ang_broadcast.value) > D_broadcast.value)
          .flatMap(x => Seq((x._1.tid, x._2.tid), (x._2.tid, x._1.tid)))
          .groupByKey()
          .filter(_._2.size == t_num_broadcast.value)
          .map(_._1)
        variables.isTO_tids.addAll(isTO_result.collect())
        variables.consistency_cost :+= (new Date().getTime - consistency_start_time) / 1000.0

        val fairness_start_time = new Date().getTime
        printf("%s *** Construct Index\n", util.timestamp2string(new Date().getTime))
        variables.quad_tree_onlineEval.build(recordRDD.collect())
        variables.fairness_cost :+= (new Date().getTime - fairness_start_time) / 1000.0

        val end_time = new Date().getTime
        val cost_time = end_time - start_time
        val delay = cost_time - variables.time_window
        variables.delays :+= (if (delay > 0) delay.toDouble / 1000 else 0.0)
        printf(" *** %.2f seconds delay ***\n", variables.delays.last)
      }
    })

    // start receiving data
    ssc.start()
    snapshot.send(ssc.sparkContext, items)
    ssc.stop(stopSparkContext = true, stopGracefully = true)

    val t1 = new Date().getTime
    val len_cal = variables.lens.values
    val mean = len_cal.sum / len_cal.size
    val std = math.sqrt(len_cal.map(x => math.pow(x - mean, 2)).sum / len_cal.size)
    val length_thresh = math.max(0, mean - 3 * std)
    val isTLC_result = variables.lens.filter(x => x._2 < length_thresh).keys
    variables.isTLC_tids.addAll(isTLC_result)
    variables.consistency_cost = variables.consistency_cost.map(_ + (new Date().getTime - t1) / (1000.0 * variables.consistency_cost.length)) // 评估长度不满足约束轨迹的时间平摊到每个时间片上

    val t2 = new Date().getTime
    val isSC_result = variables.ps.filter(x => x.length >= variables.min_pts && isSC.calculatePCT(x, variables.dcc_ap, variables.pct_ap))
    variables.isSC_tids.addAll(isSC_result.map(_.head._2.tid))
    variables.isSC_pids.addAll(isSC_result.flatMap(_.map(_._2.pid)))
    variables.consistency_cost = variables.consistency_cost.map(_ + (new Date().getTime - t2) / (1000.0 * variables.consistency_cost.length))

    val t3 = new Date().getTime
    var isSpatialUniform_result = Array[(Box, Double)]()
    for (box <- variables.boxs) {
      val cnt = variables.quad_tree_onlineEval.rangeSearch(box).length.toDouble
      val i = (cnt / variables.p_num) * (variables.geo_bound.getArea / box.getArea)
      val density = i / (i + 1)
      isSpatialUniform_result :+= (box, density)
    }
    variables.isSpatialUniform_density.addAll(isSpatialUniform_result)
    var isTemporalUniform_result = Array[(TimeSpan, Double)]()
    variables.bplus_tree_onlineEval = variables.quad_tree_onlineEval.dfsBPlusTree(variables.order)
    for (span <- variables.timespan) {
      val related =  variables.bplus_tree_onlineEval.filter(_._2.overlap(span)).map(_._1)
      val cnt = related.flatMap(x => x.boundedKeysIterator((Symbol(">="), (span.start, -1)), (Symbol("<"), (span.end + 1, -1)))).length.toDouble
      val i = (cnt / variables.p_num) * (variables.time_bound.getInterval.toDouble / span.getInterval.toDouble)
      val density = i / (i + 1)
      isTemporalUniform_result :+= (span, density)
    }
    variables.isTemporalUniform_density.addAll(isTemporalUniform_result)
    variables.fairness_cost = variables.fairness_cost.map(_ + (new Date().getTime - t3) / (1000.0 * variables.fairness_cost.length)) // 查询时空密度时间平摊到每个时间片上

    printf("\n============================== Statistics ==============================\n")
    printf(" *** Average %d points ***\n", variables.window_point_cnt.sum / variables.window_point_cnt.length)
    printf(" *** Average %.2f secs delay ***\n", variables.delays.sum / variables.delays.length)
    printf("----------\n")
    printf(" *** %d trajectories violate isInRange\n", variables.isInRange_tids.size)
    printf(" *** %d trajectories violate isOrderConstraint\n", variables.isOrderConstraint_tids.size)
    printf(" *** Average time of validity: %.3fs ***\n", variables.validity_cost.sum / variables.validity_cost.length)
    printf("----------\n")
    printf(" *** %d trajectories violate hasMP\n", variables.hasMP_tids.size)
    printf(" *** Average time of completeness: %.3fs ***\n", variables.completeness_cost.sum / variables.completeness_cost.length)
    printf("----------\n")
    printf(" *** %d trajectories violate isTLC\n", variables.isTLC_tids.size)
    if (items("bfmap") != "") {
      printf(" *** %d trajectories violate isRN\n", variables.isRN_tids.size)
    }
    printf(" *** %d trajectories violate isSC\n", variables.isSC_tids.size)
    printf(" *** %d trajectories violate isPSM\n", variables.isPSM_tids.size)
    printf(" *** %d trajectories violate isTO\n", variables.isTO_tids.size)
    printf(" *** Average time of consistency: %.3fs ***\n", variables.consistency_cost.sum / variables.consistency_cost.length)
    printf("----------\n")
    printf(" *** Average time of fairness: %.3fs ***\n", variables.fairness_cost.sum / variables.fairness_cost.length)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("traQA")
    val sc = new SparkContext(conf)

    val items = util.getArgs("config.properties")
    util.variables_init(items)
    if (items("mode") != "stream") {
      offlineEval(sc, items)
      util.display()
    }
    else {
      val ssc = new StreamingContext(sc, Milliseconds(variables.time_window))
      onlineEval(ssc, items)
      util.display()
    }
  }
}
