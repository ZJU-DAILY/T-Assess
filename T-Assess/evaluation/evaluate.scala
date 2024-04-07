package evaluation

import java.util.Date
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.max
import scala.math.min
import com.esri.core.geometry.Point
import org.apache.spark.rdd.RDD
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer
import validity._
import completeness._
import consistency._
import fairness._
import kit._
import util._

object evaluate {
  //------------------------------------------------config------------------------------------------------//
  // define arg option
  val dataset = Option.builder("dataset").argName("dataset").desc("string, url for dataset").build()
  val query = Option.builder("query").argName("query").desc("string, url for query.json").build()
  val bfmap = Option.builder("bfmap").argName("bfmap").desc("string, url for *.bfmap").build()
  val geobound = Option.builder("geobound").argName("geobound").desc("tuple4, geography bound").build()
  val sample = Option.builder("sample").argName("sample").desc("boolean, if evaluate sampled dataset").build()
  val token = Option.builder("token").argName("token").desc("boolean, true for online evaluation and false for offline evaluation").build()
  val asp = Option.builder("asp").argName("asp").desc("int, average sampling period (offline)").build()
  val atl = Option.builder("atl").argName("atl").desc("int, average trajectory length (offline)").build()
  // create options
  val options = new Options()
  options.addOption(dataset)
  options.addOption(query)
  options.addOption(bfmap)
  options.addOption(geobound)
  options.addOption(sample)
  options.addOption(token)
  options.addOption(asp)
  options.addOption(atl)

  val kafka_params = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "traQA_group"
  )
  val topics = Array("traQA_sender")

  val time_window = 60000
  val wds_max = 10

  // parameters setting
  //  val col_num = 5 // GMT, longitude, latitude, trajectory id, point id
  val cap = 10000 // maximum number of points in leaf node in quad-tree original: 3000
  val order = 5 // the branching factor (maximum number of branches in an internal node) of the B+-tree
  val p1 = 10 // coefficient, get maximum interval between two consecutive points for segmentation purpose
  val p2 = 0.01 // coefficient, get minimum trajectory length threshold

  val eps = 50 // stop detection
  val min_time = 2000 // stop detection
  val min_pts = 50 // minimum points a stop contains
  val pct_ap = 0.6
  val dcc_ap = 0.8
  val height = 3 // hierarchy height (atom level except)
  val gran = Array[Int](100, 500, 1000) // hierarchy distance granularity

  // bounds
  var len_lower_bound = 0.0
  var interval_upper_bound = 0.0
  var geo_bound: Box = _

  // map matcher
  var matcher: BroadcastMatcher = _

  // fairness relevant
  var trqRDD: RDD[TimeSpan] = _ // temporal range query
  var grqRDD: RDD[Box] = _ // geography range query

  //------------------------------------------------statistics------------------------------------------------//
  // dataset total trajectories & points
  var dt_cnt = 0
  var dp_cnt = 0L

  // qualified total trajectories & points (construct index)
  var qt_cnt = 0
  var qp_cnt = 0L

  // streaming trajectories & cumulative total points
  var st_cnt = 0
  var sp_cnt = 0L

  // dataset spatial range
  var dss_bound: Box = _
  // dataset temporal range
  var dst_bound: TimeSpan = _

  // index
  var quad_tree: QuadTree = _
  var BPlus_trees: Array[(MemoryBPlusTree[(Long, Long), Any], TimeSpan)] = _
  var segment_tree: SegmentTree = _

  // hierarchical stay point
  var hierarchy_stops = ListBuffer[Array[(Array[(GPS, Triplet)], Point)]]()

  // validity
  val isInRange_tids = mutable.Set[Int]()
  val isOrderConstraint_tids = mutable.Set[Int]()
  val isOutOfBound_tids = mutable.Set[Int]()

  // completeness
  val hasMP_tids = mutable.Set[Int]() // trajectory id unsatisfying constraint
  var hasMP_pcnt = 0L // total missing GPS point

  // consistency
  val isRN_tids = mutable.Set[Int]()
  val isSC_tids = mutable.Set[Int]()
  val isTO_tids = mutable.Set[Int]()
  val isTLC_tids = mutable.Set[Int]()
  val isPSM_tids = mutable.Set[Int]()
  val isRN_pids = mutable.Set[Long]() // GPS point id unsatisfying constraint
  val isSC_pids = mutable.Set[Long]()
  val isTO_pids = mutable.Set[Long]()
  val isTLC_pids = mutable.Set[Long]()
  val isPSM_pids = mutable.Set[Long]()

  // data structure
  val lens = mutable.Map[Int, Double]() // isTLC online
  val idles = mutable.Map[Int, Int]() // isTLC online

  // fairness
  val isSpatialUniform_den = mutable.Map[Box, Double]() // density corresponding to query content
  val isTemporalUniform_den = mutable.Map[TimeSpan, Double]()

  //------------------------------------------------offline------------------------------------------------//
  private def offlineEval(sc: SparkContext, url: String, sample:Boolean): Unit = {
    println(timestamp2string(new Date().getTime) + " *** turn dataset into RDD & get total trajectories and points")
    var recordRDD = sc.textFile(url, minPartitions = 1)
      .map(x => x.split(","))
      .map(y => (GPS(y(1).toDouble, y(2).toDouble), Triplet(y(0), y(3).toInt, y(4).toLong)))
    if (sample) {
      val lastIndex = url.lastIndexOf("/")
      val path = url.substring(0, lastIndex + 1) + "samples.txt"
      val samples = sc.textFile(path)
        .map(x => (x.toInt, true))
        .collect()
        .toMap
      recordRDD = recordRDD
        .filter(x => samples.contains(x._2.traj_id))
    }
    recordRDD.cache()
    dt_cnt = recordRDD
      .map(x => x._2.traj_id)
      .distinct()
      .count().toInt
    dp_cnt = recordRDD
      .count()
    println(" *** " + dt_cnt + " trajectories")
    println(" *** " + dp_cnt + " points")

    println(timestamp2string(new Date().getTime) + " *** evaluate & filter trajectory with isInRange")
    val resRDD_isInRange = recordRDD
      .map(x => (x, isInRange.eval(x)))
      .cache()
    isInRange_tids.addAll(resRDD_isInRange.filter(!_._2).map(x => x._1._2.traj_id).collect())
    val qualifiedRDD1 = resRDD_isInRange
      .filter(_._2)
      .map(y => y._1)
      .cache()
    val cnt1 = qualifiedRDD1.count()
    println(" *** filter " + (dp_cnt - cnt1) + " points")

    println(timestamp2string(new Date().getTime) + " *** evaluate & filter trajectory with isOutOfBound")
    val a1 = geo_bound
    val resRDD_isOutOfBound = qualifiedRDD1
      .map(x => (x, isOutOfBound.eval(x, a1)))
      .cache()
    isOutOfBound_tids.addAll(resRDD_isOutOfBound.filter(!_._2).map(x => x._1._2.traj_id).collect())
    val qualifiedRDD2 = resRDD_isOutOfBound
      .filter(_._2)
      .map(y => y._1)
      .cache()
    val cnt2 = qualifiedRDD2.count()
    println(" *** filter " + (cnt1 - cnt2) + " points")

    println(timestamp2string(new Date().getTime) + " *** split trajectory according to time interval & space span")
    val a2 = interval_upper_bound
    val splitRDD = qualifiedRDD2
      .map(x => (x._2.traj_id, x))
      .groupByKey()
      .map(y => segment(y._2.toArray, a2))
      .flatMap(z => z)
      .cache()
    println(" *** " + splitRDD.count() + " sub trajectories")

    println(timestamp2string(new Date().getTime) + " *** evaluate & filter trajectory with isOrderConstraint")
    val resRDD_isOrderConstraint = splitRDD
      .map(x => (x, isOrderConstraint.eval(x)))
      .cache()
    isOrderConstraint_tids.addAll(resRDD_isOrderConstraint.filter(!_._2).map(x => x._1.head._2.traj_id).collect())
    val qualifiedRDD3 = resRDD_isOrderConstraint
      .filter(_._2)
      .map(y => y._1)
      .cache()
    val cnt3 = qualifiedRDD3.count()
    println(" *** filter " + (splitRDD.count() - cnt3) + " sub trajectories")

    println(timestamp2string(new Date().getTime) + " *** update trajectory id and point id & get total qualified trajectories and points & get dataset bounds")
    val updateRDD = qualifiedRDD3
      .zipWithIndex()
      .map(x => x._1.map(i => (i._1, Triplet(i._2.GMT, x._2.toInt, i._2.poi_id))))
      .repartition(500)
      .cache()
    qt_cnt = updateRDD
      .count().toInt
    val flatRDD = updateRDD
      .flatMap(x => x)
      .zipWithIndex()
      .map(y => (y._1._1, Triplet(y._1._2.GMT, y._1._2.traj_id, y._2)))
      .cache()
    qp_cnt = flatRDD
      .count()
    println(" *** " + qt_cnt + " qualified trajectories")
    println(" *** " + qp_cnt + " qualified points")
    val GMTRDD = flatRDD
      .map(x => x._2.GMT)
      .cache()
    dst_bound = TimeSpan(string2timestamp(GMTRDD.min), string2timestamp(GMTRDD.max))
    val lonRDD = flatRDD
      .map(y => y._1.getLon)
      .cache()
    val latRDD = flatRDD
      .map(y => y._1.getLat)
      .cache()
    dss_bound = Box(lonRDD.min, latRDD.max, lonRDD.max, latRDD.min)

    println(timestamp2string(new Date().getTime) + " *** evaluate qualified trajectories with hasMP")
    val hasMPRDD = updateRDD
      .map(x => (x, hasMP.evalOff(x)))
      .filter(_._2 != 0)
      .cache()
    hasMP_tids.addAll(hasMPRDD.map(x => x._1.head._2.traj_id).collect())
    hasMP_pcnt += hasMPRDD.map(x => x._2.toLong).collect().sum

    println(timestamp2string(new Date().getTime) + " *** evaluate qualified trajectories with isTLC")
    val a3 = len_lower_bound
    val isTLCRDD = updateRDD
      .map(x => (x, isTLC.evalOff(x, a3)))
      .filter(!_._2)
      .map(y => y._1)
      .cache()
    isTLC_tids.addAll(isTLCRDD.map(x => x.head._2.traj_id).collect())
    isTLC_pids.addAll(isTLCRDD.flatMap(x => x.map(y => y._2.poi_id)).collect())

    println(timestamp2string(new Date().getTime) + " *** evaluate qualified trajectories with isRN")
//    val map_matcher = sc.broadcast(matcher)
//    val res_isRN = updateRDD
//      .map(x => (x, isRN.evalOff(x, map_matcher)))
//      .filter(!_._2._1)
//      .map(y => (y._1, y._2._2))
//      .cache()
//    isRN_tids.addAll(res_isRN.map(x => x._1.head._2.traj_id).collect())
//    isRN_pids.addAll(res_isRN.flatMap(x => x._2).collect())


    println(timestamp2string(new Date().getTime) + " *** get possible stops")
    val stopsRDD = updateRDD
      .map(x => isSC.detectStopWithGeoOff(x))
      .flatMap(y => y)
      .cache()
    println(" *** " + stopsRDD.count() + " possible stops")

    println(timestamp2string(new Date().getTime) + " *** construct stop hierarchy and tell outliers")
    val psRDD_index = stopsRDD
      .map(x => (x, x.length >= min_pts && isSC.calculatePCT(x)))
      .filter(_._2)
      .map(y => (y._1, isSC.centerPoint(y._1)))
      .zipWithIndex()
      .cache()
    val batch100 = 100
    var s0 = 0
    var e0 = batch100
    var temp = Array[(Array[(GPS, Triplet)], Point)]()
    while (s0 < psRDD_index.count()) {
      val batch = psRDD_index
        .filter(x => x._2 >= s0 && x._2 < e0)
        .map(y => y._1)
        .collect()
      temp = temp.appendedAll(batch)
      s0 = e0
      e0 = s0 + batch100
    }
    evaluate.hierarchy_stops.append(temp)
//    println(" *** level 0: " + evaluate.hierarchy_stops.head.length + " atom stops")
    isSC.hierarchyDiscover(evaluate.hierarchy_stops.head, 1)
    val isSCRDD = sc.makeRDD(hierarchy_stops.last.map(x => x._1))
      .map(x => x.map(y => (y._2.traj_id, y._2.poi_id)).groupBy(_._1))
      .filter(_.size == 1)
      .cache()
    isSC_tids.addAll(isSCRDD.flatMap(x => x.keys).collect())
    isSC_pids.addAll(isSCRDD.flatMap(x => x.values.flatMap(y => y.map(z => z._2))).collect())

    println(timestamp2string(new Date().getTime) + " *** kalman filter noise detecting")
//    isPSM.evalOff()

    println(timestamp2string(new Date().getTime) + " *** get edge trajectories and tell outliers")
//    isTO.evalOff()
//    println(" *** " + isTO.cnt + " edge trajectories")

    println(timestamp2string(new Date().getTime) + " *** construct quad-tree index")
    val flatRDD_index = flatRDD
      .zipWithIndex()
      .cache()
    val batch10000 = 10000
    var s1 = 0
    var e1 = batch10000
    while (s1 < flatRDD.count()) {
      val batch = flatRDD_index
        .filter(x => x._2 >= s1 && x._2 < e1)
        .map(y => y._1)
        .collect()
      quad_tree.build(batch)
      s1 = e1
      e1 = s1 + batch10000
    }

    println(timestamp2string(new Date().getTime) + " *** construct BPlus-tree per node")
    BPlus_trees = quad_tree.dfsBPlusTree(order)
    println(" *** " + BPlus_trees.length + " valid quad-tree leaves")

    println(timestamp2string(new Date().getTime) + " *** construct segment-tree index")
    segment_tree = new SegmentTree(dst_bound, BPlus_trees)

    println(timestamp2string(new Date().getTime) + " *** do spatial sparseness query")
    val a4 = grqRDD
    val b4 = sc.broadcast(quad_tree)
    val c4 = qp_cnt
    val d4 = geo_bound
    val res_isSpatialUniform = isSpatialUniform.eval(a4, b4, c4, d4)
    isSpatialUniform_den.addAll(res_isSpatialUniform)

    println(timestamp2string(new Date().getTime) + " *** do temporal sparseness query")
//    val a5 = trqRDD
//    val b5 = sc.broadcast(segment_tree)
//    val c5 = sc.broadcast(BPlus_trees)
//    val d5 = qp_cnt
//    val e5 = dst_bound
//    val res_isTemporalUniform = isTemporalUniform.eval(a5, b5, c5, d5, e5)
//    isTemporalUniform_den.addAll(res_isTemporalUniform)

    println(timestamp2string(new Date().getTime) + " *** done")
  }

  //------------------------------------------------online------------------------------------------------//
  private val last_window = mutable.Map[Int, Array[(GPS, Triplet)]]()
  private var start_time = 0: Long
  private var end_time = 0: Long
  private var delays = Array[Double]()
  private val heaps = mutable.Map[Int, mutable.PriorityQueue[(Int, Int)]]() //hasMP
  val groups = mutable.Map[Int, Array[(GPS, Triplet)]]() // isSC

  private def onlineEval(ssc: StreamingContext, url: String, sample: Boolean): Unit = {
    var window_cnt = 0

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      Subscribe[String, String](topics, kafka_params)
    )
    val inputStream = stream.map(transform)

    inputStream.foreachRDD(RDD => {
      window_cnt += 1
      start_time = new Date().getTime
      println("----------------------------------------\n" +
        "Time: " + start_time + " ms\n" +
        "----------------------------------------")
      val in = RDD.count()
      println(in)
      if (in != 0) {
//        sp_cnt += in
//        if (st_cnt == 0) {
//          val ids = RDD
//            .map(x => x(3).toInt)
//            .distinct()
//            .collect()
//          isTLC.initState(ids)
//          isSC.initState(ids)
//          st_cnt = ids.length
//        }
//
//        val formatRDD = RDD
//          .map(x => (GPS(x(1).toDouble, x(2).toDouble), Triplet(x(0), x(3).toInt, x(4).toLong)))
//          .cache()
//
//        // isInRange
//        val isInRangeRDD = formatRDD
//          .map(x => (x, isInRange.eval(x)))
//        isInRange_tids.addAll(isInRangeRDD.filter(!_._2).map(x => x._1._2.traj_id).distinct().collect())
//
//        // isOutOfBound
//        val a1 = geo_bound
//        val isOutOfBoundRDD = isInRangeRDD
//          .map(x => (x._1, isOutOfBound.eval(x._1, a1)))
//        isOutOfBound_tids.addAll(isOutOfBoundRDD.filter(!_._2).map(x => x._1._2.traj_id).distinct().collect())
//
//        val qualifiedRDD1 = isOutOfBoundRDD
//          .filter(_._2)
//          .map(z => (z._1._2.traj_id, z._1))
//          .groupByKey()
//          .map(i => (i._1, i._2.toArray.sortBy(_._2.poi_id)))
//          .cache()
//
//        // isOrderConstraint
//        val isOrderConstraintRDD = qualifiedRDD1
//          .map(x => (x, isOrderConstraint.eval((if (last_window.contains(x._1)) Array(last_window(x._1).last) else Array()) ++ x._2)))
//          .cache()
//        isOrderConstraint_tids.addAll(isOrderConstraintRDD.filter(!_._2).map(x => x._1._1).collect())
//        val qualifiedRDD2 = isOrderConstraintRDD
//          .filter(_._2)
//          .map(y => y._1)
//          .cache()
//
//        val flatRDD = qualifiedRDD2
//          .flatMap(x => x._2)
//          .cache()
//        qp_cnt += flatRDD.count()
//
//        val GMTs = flatRDD
//          .map(x => x._2.GMT)
//          .cache()
//        if (dst_bound == null) {
//          dst_bound = TimeSpan(string2timestamp(GMTs.min), string2timestamp(GMTs.max))
//        } else {
//          dst_bound = TimeSpan(min(dst_bound.start, string2timestamp(GMTs.min)), max(dst_bound.end, string2timestamp(GMTs.max)))
//        }
//        val lons = flatRDD
//          .map(x => x._1.getLon)
//          .cache()
//        val lats = flatRDD
//          .map(x => x._1.getLat)
//          .cache()
//        if (dss_bound == null) {
//          dss_bound = Box(lons.min, lats.max, lons.max, lats.min)
//        } else {
//          dss_bound = Box(
//            min(dss_bound.left_bound, lons.min),
//            max(dss_bound.upper_bound, lats.max),
//            max(dss_bound.right_bound, lons.max),
//            min(dss_bound.lower_bound, lats.min)
//          )
//        }
//
//        quad_tree.build(flatRDD.collect())
//        BPlus_trees = quad_tree.dfsBPlusTree(order)
//        segment_tree = new SegmentTree(dst_bound, BPlus_trees)
//
//        // hasMP
//        val intervalsRDD = qualifiedRDD2
//          .map(x => (x._1, hasMP.getIntervals((if (last_window.contains(x._1)) Array(last_window(x._1).last) else Array()) ++ x._2)))
//          .filter(_._2.nonEmpty)
//          .cache()
//        for (i <- intervalsRDD.collect()) {
//          i._2.foreach(j => {
//            if (!heaps.contains(i._1)) {
//              heaps(i._1) = mutable.PriorityQueue[(Int, Int)]((1, j))
//            } else {
//              if (!heaps(i._1).exists(i => i._2 == j)) {
//                heaps(i._1).enqueue((1, j))
//              } else {
//                var obj = (-1, -1)
//                for (i <- heaps(i._1)) {
//                  if (i._2 == j) obj = i
//                }
//                heaps(i._1) = heaps(i._1).filter(_._2 != j)
//                heaps(i._1).enqueue((obj._1 + 1, obj._2))
//              }
//            }
//          })
//        }
//        val a2 = heaps
//        val hasMPRDD = intervalsRDD
//          .map(x => (x._1, hasMP.getCnt(x._1, x._2, a2)))
//          .filter(_._2 != 0)
//          .cache()
//        hasMP_tids.addAll(hasMPRDD.map(x => x._1).collect())
//        hasMP_pcnt += hasMPRDD.map(x => x._2.toLong).collect().sum
//
//        // isTLC
//        isTLC.updateState()
//        val isTLCRDD = qualifiedRDD2
//          .map(x => (x._1, isTLC.evalOn((if (last_window.contains(x._1)) Array(last_window(x._1).last) else Array()) ++ x._2)))
//          .cache()
//        for (i <- isTLCRDD.collect()) {
//          evaluate.idles(i._1) = 0
//          evaluate.lens(i._1) += i._2
//        }
//        isTLC.checkState()
//
//        // isRN
//        val a3 = ssc.sparkContext.broadcast(matcher)
//        val b3 = BroadcastMatcher.MatcherKStates
//        val isRNRDD = qualifiedRDD2
//          .map(x => (x._1, isRN.evalOn(x._2, a3, b3(x._1))))
//          .cache()
//        val mksRDD = isRNRDD
//          .map(x => (x._1, x._2._3))
//        for (i <- mksRDD.collect()) { BroadcastMatcher.MatcherKStates.update(i._1, i._2) }
//        val isRNRDD_append = isRNRDD
//          .filter(!_._2._1)
//          .map(y => (y._1, y._2._2))
//          .cache()
//        isRN_tids.addAll(isRNRDD_append.map(x => x._1).collect())
//        isRN_pids.addAll(isRNRDD_append.flatMap(x => x._2).collect())
//
//        // isSC
//        val gs = groups
//        val isSCRDD = qualifiedRDD2
//          .map(x => (x._1, isSC.detectStopWithGeoOn(x._2, gs(x._1))))
//          .cache()
//        for (i <- isSCRDD.map(x => (x._1, x._2._2)).collect()) {
//          groups.put(i._1, i._2)
//        }
//        val ps = isSCRDD
//          .map(x => x._2._1)
//          .flatMap(y => y)
//          .map(z => (z, z.length >= min_pts && isSC.calculatePCT(z)))
//          .filter(_._2)
//          .map(i => (i._1, isSC.centerPoint(i._1)))
//          .collect()
//        evaluate.hierarchy_stops.append(ps)
////        println(" *** level 0: " + evaluate.hierarchy_stops.head.length + " atom stops")
//        isSC.hierarchyDiscover(evaluate.hierarchy_stops.head, 1)
//        val levelRDD = ssc.sparkContext.makeRDD(hierarchy_stops.last.map(x => x._1))
//          .map(x => x.map(y => (y._2.traj_id, y._2.poi_id)).groupBy(_._1))
//          .filter(_.size == 1)
//          .cache()
//        isSC_tids.addAll(levelRDD.flatMap(x => x.keys).collect())
//        isSC_pids.addAll(levelRDD.flatMap(x => x.values.flatMap(y => y.map(z => z._2))).collect())
//
//        // isPSM
//        // isPSM.evalOn()
//
//        // isTO
//        // isTO.evalOn()
//
//        val a4 = grqRDD
//        val b4 = ssc.sparkContext.broadcast(quad_tree)
//        val c4 = qp_cnt
//        val d4 = geo_bound
//        isSpatialUniform.eval(a4, b4, c4, d4)
//        val a5 = trqRDD
//        val b5 = ssc.sparkContext.broadcast(segment_tree)
//        val c5 = ssc.sparkContext.broadcast(BPlus_trees)
//        val d5 = qp_cnt
//        val e5 = dst_bound
//        isTemporalUniform.eval(a5, b5, c5, d5, e5)
//
//        display(token = true)

//        // cache the last window data
//        last_window.addAll(qualifiedRDD1.collect())
      }

      end_time = new Date().getTime
      val t = end_time - start_time - time_window
      val delay = if (t > 0) t.toDouble / 1000 else 0.0
      delays :+= delay
      println("*** delay: " + "%.2f".format(delay) + " seconds ***")
      println("*** average delay: " + delays.sum / (1000 * delays.length) + " seconds ***")
      println("*** window cnt: " + window_cnt + " ***")
    })

    // start receiving data
    ssc.start()
    snapshot.send(ssc.sparkContext, url, sample)
    while (!snapshot.executor_service.isTerminated) { /* idle until all data are sent */ }
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//      .setAppName("traQA")
//      .setMaster("spark://10.214.151.197:7077")
//      .set("spark.jars", "/home/wt/code/traQA/trajectory-quality-assessment/out/artifacts/traQA_jar/traQA.jar")
//      .set("spark.driver.host", "10.214.151.197")
//      .set("spark.default.parallelism", "500")
//      .set("spark.executor.memory", "20g")

    val conf = new SparkConf()
      .setAppName("traQA")
      .setMaster("local[24]")

    val sc = new SparkContext(conf)

    val items = checkArgs(args)
    init(sc, items)
    val token = items("token").toBoolean
    if (!token) {
      offlineEval(sc, items("dataset"), items("sample").toBoolean)
      display(token)
    } else {
      val ssc = new StreamingContext(sc, Milliseconds(time_window))
      onlineEval(ssc, items("dataset"), items("sample").toBoolean)
    }
  }
}
