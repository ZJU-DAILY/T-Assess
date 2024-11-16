package kit

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.nio.file.{Files, Paths}
import java.util.stream.Collectors
import scala.collection.mutable
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import com.bmwcarit.barefoot.matcher.MatcherKState
import evaluation.variables

object snapshot {
  private def readTrajectory(sc: SparkContext, items: Map[String, String]): Map[String, Iterable[String]] = {
    val records = sc.textFile(items("dataset"), minPartitions = variables.num_partition).persist(StorageLevel.MEMORY_ONLY)
    val GMTs = records.map(x => util.string2timestamp(x.split(",").head)).persist(StorageLevel.MEMORY_ONLY)
    variables.time_bound = TimeSpan(GMTs.min(), GMTs.max)
    var trajs = records.map(x => (x.split(",")(3), x)).coalesce(1)
      .groupByKey().repartition(variables.num_partition)
    if (items("sample") != "") {
      val ids = Files.lines(Paths.get(items("sample"))).skip(5).collect(Collectors.toList())
      trajs = trajs.filter(x => ids.contains(x._1))
    }
    //  trajs = trajs.sample(withReplacement = false, fraction = 0.01, seed = 1).persist(StorageLevel.MEMORY_ONLY)
    trajs = trajs.persist(StorageLevel.MEMORY_ONLY)
    variables.t_num = trajs.count().toInt
    variables.p_num = trajs.mapPartitions(iter => Iterator(iter.map(_._2.size).sum)).sum().toLong
    trajs.collect().toMap
  }

  private def sendTrajectory(kafkaProducer: KafkaProducer[String,String], units: Array[String]): Unit = {
    val step_size = math.max(math.ceil(units.length.toDouble / 60).toInt, 1)
    val iterator = units.sliding(step_size, step_size) // 保证在60个批次传输完所有数据
    for (window <- iterator) {
      for (record <- window) {
        val message = new ProducerRecord[String, String]("trajSender", null, record)
        kafkaProducer.send(message)
        // for (_ <- 0 until 10) {
          // kafkaProducer.send(message)
        // }
      }
      Thread.sleep(variables.time_window)
    }
  }

  def send(sc: SparkContext, items: Map[String, String]): Unit = {
    val trajs = readTrajectory(sc, items)

    // zookeeper connection properties
    val props = new java.util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

    for ((key, _) <- trajs) {
      variables.heaps.put(key.toInt, mutable.PriorityQueue[(Int, Int)]())
      variables.lens.put(key.toInt, 0)
      variables.matcherKStates.put(key.toInt, new MatcherKState())
      variables.stop_groups.put(key.toInt, Array())
    }

    printf(" *** %d trajectories total\n", variables.t_num)
    printf(" *** %d points total\n", variables.p_num)
    printf("Start transmitting data\n")
    val executor_service: ExecutorService = Executors.newFixedThreadPool(trajs.size)
    for (trajectory <- trajs.values) {
      executor_service.execute(() => {
        sendTrajectory(producer, trajectory.toArray)
      })
    }
    executor_service.shutdown()
    while (!executor_service.isTerminated) {
      /* idle until all data are sent */
    }
  }
}
