package kit

import java.text.SimpleDateFormat
import java.util.Date
import java.util.Properties
import java.io.{File, FileInputStream, FileReader, InputStreamReader}
import scala.jdk.CollectionConverters._
import scala.math.max
import scala.math.min
import com.alibaba.fastjson2.JSON
import com.bmwcarit.barefoot.spatial.Geography
import com.esri.core.geometry.Point
import evaluation._

case class Triplet(var GMT: String, var tid: Int, var pid: Long)

case class GPS(x: Double, y: Double) {
  // Comparison <op>, returns true if both x and y are true for <op>
  def < (other: GPS): Boolean = x < other.x && y < other.y
  def > (other: GPS): Boolean = x > other.x && y > other.y
  def <= (other: GPS): Boolean = x <= other.x && y <= other.y
  def >= (other: GPS): Boolean = x >= other.x && y >= other.y

  def toPoint: Point = new Point(x, y)
  def getLon: Double = x
  def getLat: Double = y
}

case class Box(left_bound: Double, upper_bound: Double, right_bound: Double, lower_bound: Double){
  private val center = GPS((left_bound + right_bound) / 2, (upper_bound + lower_bound) / 2)

  val topLeftPoint: GPS = GPS(left_bound, upper_bound)
  val topRightPoint: GPS = GPS(right_bound, upper_bound)
  val bottomLeftPoint: GPS = GPS(left_bound, lower_bound)
  val bottomRightPoint: GPS = GPS(right_bound, lower_bound)

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

  override def toString: String = util.timestamp2string(start) + " ~ " + util.timestamp2string(end)
}

object util {
  def timestamp2string(tm: Long): String = {
    val ft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    ft.setLenient(false)
    ft.format(new Date(tm))
  }

  def string2timestamp(str: String): Long = {
    val ft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    ft.setLenient(false)
    ft.parse(str).getTime
  }

  def haversine(a: Point, b: Point): Double = new Geography().distance(a, b)

  def getArgs(url: String): Map[String, String] = {
    val properties = new Properties()
    try {
      val input = new FileInputStream(url)
      try {
        properties.load(input)
      } finally {
        input.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    properties.asScala.toMap
  }

  private def getQueryContent(url: String): (Array[TimeSpan], Array[Box]) = {
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

    val m = jsonObj.getJSONArray("time span")
      .toArray()
      .map(x => {
        val items = x.toString.split("~")
        val st = util.string2timestamp(items(0))
        val ed = util.string2timestamp(items(1))
        TimeSpan(st, ed)
      })
    val n = jsonObj.getJSONArray("query box")
      .toArray()
      .map(x => {
        val items = x.toString.split(",")
        Box(items(0).toDouble, items(1).toDouble, items(2).toDouble, items(3).toDouble)
      })
    (m, n)
  }

  def variables_init(items: Map[String, String]): Unit = {
    val qc = getQueryContent(items("query"))
    variables.timespan = qc._1
    variables.boxs = qc._2

    if (items("bfmap") != "") {
      // Instantiate map matcher as broadcast variable in Spark Context (sc)
      variables.map_matcher = new BroadcastMatcher(items("bfmap"))
    }

    val gb = items("geo_bound").substring(1, items("geo_bound").length - 1).split(",").map(_.toDouble)
    variables.geo_bound = Box(gb(0), gb(1), gb(2), gb(3))
    if (items("mode") == "stream") {
      variables.quad_tree_onlineEval = new QuadTree(variables.cap, variables.geo_bound)
    }

    if (items("mode") == "local") {
      variables.num_partition = 100
    } else {
      variables.num_partition = 1000
    }
  }

  def display(): Unit = {
    println(
      "================================ Report ================================\n" +
      "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
      "-spatial range-\n" +
        "  longitude: " + variables.geo_bound.left_bound + " ~ " + variables.geo_bound.right_bound + "\n" +
        "  latitude: " + variables.geo_bound.lower_bound + " ~ " + variables.geo_bound.upper_bound + "\n" +
      "-temporal range-\n" +
        "  " + variables.time_bound.toString + "\n" +
      "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ validity ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
        "validity score V0: " + "%.2f".format(1 - (variables.isInRange_tids ++ variables.isOrderConstraint_tids).size.toDouble / variables.t_num)  + "\n" +
      "~~~~~~~~~~~~~~~~~~~~~~~~~~~ completeness ~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
        "completeness score C0: " + "%.2f".format(1 - variables.hasMP_tids.size.toDouble / variables.t_num) + "\n" +
        "ratio for datasets to ensure completeness: " + "%.2f".format(variables.hasMP_pcnt.toDouble / variables.p_num) + "\n" +
      "~~~~~~~~~~~~~~~~~~~~~~~~~~~~ consistency ~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
      "consistency score C0: " + "%.2f".format(1 - (variables.isRN_tids ++ variables.isTO_tids ++ variables.isSC_tids ++ variables.isTLC_tids ++ variables.isPSM_tids).size.toDouble / variables.t_num) + "\n" +
      "consistency score C1: " + "%.2f".format(1 - (variables.isRN_pids ++ variables.isSC_pids ++ variables.isPSM_pids).size.toDouble / variables.p_num)
    )
    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ fairness ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    println("-isSpatialUniform-")
    variables.isSpatialUniform_density.foreach(x => {
      printf("  %-8s : %-8s\n", x._1.toString, "%.5f".format(x._2))
    }) + "\n"
    println("-isTemporalUniform-")
    variables.isTemporalUniform_density.foreach(x => {
      printf("  %-8s : %-8s\n", x._1.toString, "%.5f".format(x._2))
    })
  }
}