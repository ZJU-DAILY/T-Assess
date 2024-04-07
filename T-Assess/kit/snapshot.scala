package kit

import java.util.HashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import scala.collection.mutable
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkContext

object snapshot {
  val executor_service: ExecutorService = Executors.newFixedThreadPool(2000)
  private val hash = mutable.Map[Int, Array[String]]()

  private def readTrajectory(sc: SparkContext, url: String, sample: Boolean): Unit = {
    var trajs = sc.textFile(url)
      .map(x => (x.split(",")(3), x))
      .groupByKey()
      .collect()
    var samples = Map[Int, Boolean]()
    if (sample) {
      val lastIndex = url.lastIndexOf("/")
      val path = url.substring(0, lastIndex + 1) + "samples.txt"
      samples = sc.textFile(path)
        .map(x => (x.toInt, true))
        .collect()
        .toMap
      trajs = trajs.filter(x => samples.contains(x._1.toInt))
    }
    println(trajs.flatMap(x => x._2).length)
    for (i <- trajs) {
      hash.put(i._1.toInt, i._2.toArray)
    }
  }

  private def sendTrajectory(kafkaProducer: KafkaProducer[String,String], topic: String, units: Array[String]): Unit = {
    for (i <- units.indices) {
      val message = new ProducerRecord[String, String](topic, null, units(i))
      kafkaProducer.send(message)
      if (i != units.length - 1) {
        val s1 = units(i).split(",")(0)
        val s2 = units(i + 1).split(",")(0)
        val interval = util.string2timestamp(s2) - util.string2timestamp(s1)
        if (interval >= 0) Thread.sleep(interval)
      }
    }
  }

  def send(sc: SparkContext, url: String, sample: Boolean): Unit = {
    readTrajectory(sc, url, sample)
    val brokers = "localhost:9092"
    val topics = "traQA_sender"
    // zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    // trajectory parallel transmission
    for (trajectory <- hash) {
      executor_service.execute(() => {
        sendTrajectory(producer, topics, trajectory._2)
      })
    }
    executor_service.shutdown()
  }
}
