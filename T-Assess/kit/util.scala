package kit

import java.text.SimpleDateFormat
import java.util.Date
import java.io.{File, FileInputStream, FileReader, InputStreamReader}
import scala.math.max
import scala.math.min
import scala.collection.mutable
import com.alibaba.fastjson2.JSON
import com.bmwcarit.barefoot.spatial.Geography
import com.esri.core.geometry.Point
import org.apache.spark.SparkContext
import org.apache.commons.cli.DefaultParser
import org.apache.kafka.clients.consumer.ConsumerRecord
import evaluation._

// Comparison <op>, returns true if both x and y are true for <op>
case class GPS(x: Double, y: Double) {
  def < (other: GPS): Boolean = x < other.x && y < other.y
  def > (other: GPS): Boolean = x > other.x && y > other.y
  def <= (other: GPS): Boolean = x <= other.x && y <= other.y
  def >= (other: GPS): Boolean = x >= other.x && y >= other.y

  def toPoint: Point = new Point(x, y)

  def getLat: Double = y
  def getLon: Double = x
}

case class Box(left_bound: Double, upper_bound: Double, right_bound: Double, lower_bound: Double){
  private val center = GPS((left_bound + right_bound) / 2, (upper_bound + lower_bound) / 2)
  private val halfDimX = (right_bound - left_bound) / 2
  private val halfDimY = (upper_bound - lower_bound) / 2

  val topLeftPoint: GPS = GPS(center.x - halfDimX, center.y + halfDimY)
  val topRightPoint: GPS = GPS(center.x + halfDimX, center.y + halfDimY)
  val bottomLeftPoint: GPS = GPS(center.x - halfDimX, center.y - halfDimY)
  val bottomRightPoint: GPS = GPS(center.x + halfDimX, center.y - halfDimY)

  def topLeftSubBox: Box = Box(topLeftPoint.x, topLeftPoint.y, center.x, center.y)
  def topRightSubBox: Box = Box(center.x, topRightPoint.y, topRightPoint.x, center.y)
  def bottomLeftSubBox: Box = Box(bottomLeftPoint.x, center.y, center.x, bottomLeftPoint.y)
  def bottomRightSubBox: Box = Box(center.x, center.y, bottomRightPoint.x, bottomRightPoint.y)

  def contain(p: GPS): Boolean = p > bottomLeftPoint && p <= topRightPoint
  def overlap(r: Box): Boolean = bottomLeftPoint <= r.topRightPoint && topRightPoint >= r.bottomLeftPoint

  def getArea: Double = (right_bound - left_bound) * (upper_bound - lower_bound)

  override def toString: String = "[" + left_bound + ", " + upper_bound + ", " + right_bound + ", " + lower_bound + "]"
}

case class TimeSpan(start: Long, end: Long) {
  def overlap(r: TimeSpan): Boolean = max(start, r.start) <= min(end, r.end)

  def getInterval: Int = ((end - start) / 1000).toInt

  override def toString: String = {
    util.timestamp2string(start) + " ~ " + util.timestamp2string(end)
  }
}

case class Triplet(var GMT: String, var traj_id: Int, var poi_id: Long)

object util {
  def timestamp2string(tm: Long): String = {
    val ft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    ft.setLenient(false)
    ft.format(new Date(tm))
  }

  def string2timestamp(str: String): Long = {
    val ft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    ft.setLenient(false)
    ft.parse(str).getTime
  }

  def haversine(a: Point, b: Point): Double = new Geography().distance(a, b)

  def checkArgs(args: Array[String]): Map[String, String] = {
    try {
      val parser = new DefaultParser()
      val cmd = parser.parse(evaluate.options, args)
      if (cmd.getArgs.length != 8) {
        throw new Exception()
      }
      val arg_list = cmd.getArgs
      val mp = mutable.Map[String, String]()
      mp.put("dataset", arg_list(0))
      mp.put("query", arg_list(1))
      mp.put("bfmap", arg_list(2))
      mp.put("geobound", arg_list(3))
      mp.put("sample", arg_list(4))
      mp.put("token", arg_list(5))
      mp.put("asp", arg_list(6))
      mp.put("atl", arg_list(7))
      mp.toMap
    } catch {
      case _: Exception =>
        println("please check args:")
        evaluate.options.getOptions.forEach(option => {
          println("-" + option.getArgName + ": " + option.getDescription)
        })
        System.exit(0)
        Map()
    }
  }

  def init(sc: SparkContext, items: Map[String, String]): Unit = {
    def queryContent(url: String): (Array[TimeSpan], Array[Box]) = {
      val file = new File(url)
      val fileReader = new FileReader(file)
      val reader = new InputStreamReader(new FileInputStream(file), "Utf-8")
      var ch = 0
      val sb = new StringBuffer()
      do {
        ch = reader.read()
        if (ch != -1) sb.append(ch.toChar)
      } while (ch != -1)
      reader.close()
      fileReader.close()
      val jsonStr = sb.toString
      val jsonObj = JSON.parseObject(jsonStr)

      val m = jsonObj
        .getJSONArray("time span")
        .toArray()
        .map(x => {
          val items = x.toString.split("~")

          val st = util.string2timestamp(items(0))
          val ed = util.string2timestamp(items(1))

          TimeSpan(st, ed)
        })

      val n = jsonObj
        .getJSONArray("query box")
        .toArray()
        .map(x => {
          val items = x.toString.split(",")
          Box(items(0).toDouble, items(1).toDouble, items(2).toDouble, items(3).toDouble)
        })

      (m, n)
    }

    val content = queryContent(items("query"))
    content._1.foreach(x => evaluate.isTemporalUniform_den.put(x, 0))
    content._2.foreach(x => evaluate.isSpatialUniform_den.put(x, 0))
    evaluate.trqRDD = sc.makeRDD(content._1)
    evaluate.grqRDD = sc.makeRDD(content._2)

    evaluate.matcher = new BroadcastMatcher(items("bfmap"))// Instantiate map matcher as broadcast variable in Spark Context (sc)

    val m = items("geobound").substring(1, items("geobound").length - 1).split(",")
    evaluate.geo_bound = Box(m(0).toDouble, m(1).toDouble, m(2).toDouble, m(3).toDouble)

    evaluate.quad_tree = new QuadTree(evaluate.cap, evaluate.geo_bound)

    if (items.contains("asp")) evaluate.interval_upper_bound = items("asp").toDouble * evaluate.p1
    if (items.contains("atl")) evaluate.len_lower_bound = items("atl").toDouble * evaluate.p2
  }

  def display(token: Boolean): Unit = {
    println(
      "===============================Report===============================\n" +
      "~~~~~~~~~~~~~~~~~~~~~~Info~~~~~~~~~~~~~~~~~~~~~~\n" +
      "-spatial range-\n" +
      "  longitude: " + evaluate.dss_bound.left_bound + " ~ " + evaluate.dss_bound.right_bound + "\n" +
      "  latitude: " + evaluate.dss_bound.lower_bound + " ~ " + evaluate.dss_bound.upper_bound + "\n" +
      "-temporal range-\n" +
      "  " + evaluate.dst_bound.toString + "\n" +
      "~~~~~~~~~~~~~~~~~~~~validity~~~~~~~~~~~~~~~~~~~~\n" +
      "-isInRange-\n" +
      //        "  " + statistics.isInRange_tids.mkString("unsatisfied with isInRange: ", ", ", ".") + "\n" +
      "  cnt: " + evaluate.isInRange_tids.size + "\n" +
      "-isOutOfBound-\n" +
      //        "  " + statistics.isOutOfBound_tids.mkString("unsatisfied with isOutOfBound: ", ", ", ".") + "\n" +
      "  cnt: " + evaluate.isOutOfBound_tids.size + "\n" +
      "-isOrderConstraint-\n" +
      //        "  " + statistics.isOrderConstraint_tids.mkString("unsatisfied with isOrderConstraint: ", ", ", ".") + "\n" +
      "  cnt: " + evaluate.isOrderConstraint_tids.size + "\n" +
      "validity score V0: " + "%.2f".format(1 - (evaluate.isInRange_tids ++ evaluate.isOutOfBound_tids ++ evaluate.isOrderConstraint_tids).size.toDouble / (if (token) evaluate.st_cnt else evaluate.dt_cnt))  + "\n" +
      "~~~~~~~~~~~~~~~~~~completeness~~~~~~~~~~~~~~~~~~\n" +
      "-hasMP-\n" +
      //        "  " + statistics.hasMP_tids.mkString("unsatisfied with completeness: ", ", ", ".") + "\n" +
      "  cnt: " + evaluate.hasMP_tids.size + "\n" +
      "completeness score C0: " + "%.2f".format(1 - evaluate.hasMP_tids.size.toDouble / (if (token) evaluate.st_cnt else evaluate.qt_cnt)) + "\n" +
      "ratio for datasets to ensure completeness: " + "%.2f".format(evaluate.hasMP_pcnt.toDouble / evaluate.qp_cnt) + "\n" +
      "~~~~~~~~~~~~~~~~~~~consistency~~~~~~~~~~~~~~~~~~\n" +
      "-isTLC-\n" +
      //        "  " + statistics.isTLC_tids.mkString("unsatisfied with isTLCOff: ", ", ", ".") + "\n" +
      "  cnt: " + evaluate.isTLC_tids.size + "\n" +
      "-isRN-\n" +
      //        "  " + statistics.isRN_tids.mkString("unsatisfied with isRNOff: ", ", ", ".") + "\n" +
      "  cnt: " + evaluate.isRN_tids.size + "\n" +
      "-isSC-\n" +
      //        "  " + statistics.isSC_tids.mkString("unsatisfied with isSC: ", ", ", ".") + "\n" +
      "  cnt: " + evaluate.isSC_tids.size + "\n" +
      "-isTO-\n" +
      //        "  " + statistics.isTO_tids.mkString("unsatisfied with isTO: ", ", ", ".") + "\n" +
      "  cnt: " + evaluate.isTO_tids.size + "\n" +
      "-isPSM-\n" +
      //        "  " + statistics.isPSM_tids.mkString("unsatisfied with isPSM: ", ", ", ".") + "\n" +
      "  cnt: " + evaluate.isPSM_tids.size + "\n" +
      "consistency score C0: " + "%.2f".format(1 - (evaluate.isRN_tids ++ evaluate.isTO_tids ++ evaluate.isSC_tids ++ evaluate.isTLC_tids ++ evaluate.isPSM_tids).size.toDouble / (if (token) evaluate.st_cnt else evaluate.qt_cnt)) + "\n" +
      "consistency score C1: " + "%.2f".format(1 - (evaluate.isRN_pids ++ evaluate.isTO_pids ++ evaluate.isSC_pids ++ evaluate.isTLC_pids ++ evaluate.isPSM_pids).size.toDouble / evaluate.qp_cnt)
    )
    println("~~~~~~~~~~~~~~~~~~~~fairness~~~~~~~~~~~~~~~~~~~~")
    println("-isSpatialUniform-")
    evaluate.isSpatialUniform_den.foreach(x => {
      printf("  %-8s : %-8s\n", x._1.toString, "%.5f".format(x._2))
    }) + "\n" +
      println("-isTemporalUniform-")
    evaluate.isTemporalUniform_den.foreach(x => {
      printf("  %-8s : %-8s\n", x._1.toString, "%.5f".format(x._2))
    })
  }

  def segment(trajectory: Array[(GPS, Triplet)], threshold: Double): Array[Array[(GPS, Triplet)]] = {
    var ids: Array[Int] = Array(-1)
    var res: Array[Array[(GPS, Triplet)]] = Array()
    val GMTs = trajectory.map(x => x._2.GMT)
    for (i <- 0 until GMTs.length - 1) {
      val t1 = util.string2timestamp(GMTs(i))
      val t2 = util.string2timestamp(GMTs(i + 1))
      if ((t2 - t1) / 1000 > threshold) ids :+= i
    }
    ids :+= trajectory.length - 1
    for (i <- 0 until ids.length - 1) {
      var item: Array[(GPS, Triplet)] = Array()
      for (j <- (ids(i) + 1) to ids(i + 1)) item :+= trajectory(j)
      res :+= item
    }
    res
  }

  def transform(line: ConsumerRecord[String, String]): Array[String] = line.value().split(",")
}
