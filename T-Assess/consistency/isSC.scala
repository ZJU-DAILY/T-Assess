package consistency

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
import com.esri.core.geometry.Point
import kit._

object isSC {
    private def calSlowPoint(traj: Array[(Point, Triplet)], slow_point_thresh: Double): Array[(Point, Triplet)] = {
    var sp = traj
    for (i <- 1 until traj.length - 1) {
      if (util.haversine(traj(i)._1, traj(i + 1)._1) > (slow_point_thresh * ((util.string2timestamp(traj(i + 1)._2.GMT) - util.string2timestamp(traj(i)._2.GMT)) / 1000))) {
        sp = sp.filterNot(_ == traj(i + 1))
      }
    }
    sp
  }

  private def duration(point: String, neighbours: Array[String]): Long = {
    val times = neighbours.map(util.string2timestamp)
    val cmp = util.string2timestamp(point)
    val early = if (times.isEmpty) cmp else math.min(cmp, times.min)
    val late = if (times.isEmpty) cmp else math.max(cmp, times.max)
    (late - early) / 1000
  }

  private def epsLinearNeighbours(point: (Point, Triplet), point_set: Array[(Point, Triplet)], eps: Int): ListBuffer[(Point, Triplet)] = {
    ListBuffer() ++ point_set.filter(x => util.haversine(point._1, x._1) < eps)
  }

  private def findPS(point_set: Array[(Point, Triplet)], min_time: Int, eps: Int):  Array[Array[(Point, Triplet)]] = {
    var pts = point_set
    var ps = Array[Array[(Point, Triplet)]]()
    for (point <- point_set) {
      if (pts.contains(point)) {
        pts = pts.filterNot(_ == point)
        val N = epsLinearNeighbours(point, point_set, eps)
        if (duration(point._2.GMT, N.map(_._2.GMT).toArray) > min_time) {
          var C = Array[(Point, Triplet)](point)
          var index = 0
          while(index < N.size) {
            val neighbor = N(index)
            if (pts.contains(neighbor)) {
              pts = pts.filterNot(_ == neighbor)
              C ++= Array(neighbor).diff(C)
              val N1 = epsLinearNeighbours(neighbor, point_set, eps)
              if (duration(neighbor._2.GMT, N1.map(_._2.GMT).toArray) > min_time) {
                N ++= N1.diff(N)
              }
            }
            index += 1
          }
          ps :+= C
        }
      }
    }
    ps
  }

  def trajDBSCAN(traj: Iterable[(GPS, Triplet)], slow_point_thresh: Double, min_time: Int, eps: Int): Array[Array[(Point, Triplet)]] = {
    val sp = calSlowPoint(traj.map(x => (x._1.toPoint, x._2)).toArray, slow_point_thresh)
    val ps = findPS(sp, min_time, eps)
    ps
  }

  def getCenterPoint(points: Array[Point]): Point = {
    val meanLon = points.map(_.getX).sum / points.length
    val meanLat = points.map(_.getY).sum / points.length
    new Point(meanLon, meanLat)
  }

  def clusterPS(ps:  Array[Array[(Point, Triplet)]], eps: Int): Array[Array[(Int, Long)]] = {
    var tps = ps.map(x => getCenterPoint(x.map(_._1))).zipWithIndex
    val temp = tps
    var cluster = Array[Array[(Int, Long)]]()
    for (point <- temp) {
      if (tps.contains(point)) {
        tps = tps.filterNot(_ == point)
        val N = tps.filter(x => util.haversine(point._1, x._1) < eps)
        var t = ps(point._2).map(x => (x._2.tid, x._2.pid))
        for (id <- N.map(_._2)) {
          t = t ++ ps(id).map(x => (x._2.tid, x._2.pid))
        }
        cluster :+= t
        for (neighbor <- N) {
          tps = tps.filterNot(_ == neighbor)
        }
      }
    }
    cluster
  }

  private def updateGroup(group: Array[(GPS, Triplet)], eps: Int): Int = {
    for (i <- group.indices) {
      breakable {
        for (j <- i + 1 to group.length) {
          if (j == group.length) return i
          if (util.haversine(group(i)._1.toPoint, group(j)._1.toPoint) > eps) break
        }
      }
    }
    -1
  }

  def geoStopDetection(traj: Array[(GPS, Triplet)], g: Array[(GPS, Triplet)], eps: Int, min_time: Int): (ListBuffer[Array[(GPS, Triplet)]], Array[(GPS, Triplet)]) = {
    val ps = ListBuffer[Array[(GPS, Triplet)]]()
    var group = if (g.nonEmpty) g else Array(traj.head)
    val index = if (g.nonEmpty) 0 else 1
    for (i <- index until traj.length) {
      val dis = util.haversine(group.head._1.toPoint, traj(i)._1.toPoint)
      if (dis <= eps) group :+= traj(i)
      else {
        val interval = (util.string2timestamp(group.last._2.GMT) - util.string2timestamp(group.head._2.GMT)) / 1000
        if (interval > min_time) {
          ps += group
          group = Array(traj(i))
        }
        else {
          group = group.drop(1) :+ traj(i)
          val pos = updateGroup(group, eps)
          assert(pos != -1)
          group = group.drop(pos)
        }
      }
    }
    (ps, group)
  }

  def calculatePCT(stop: Array[(GPS, Triplet)], dcc_ap: Double, pct_ap: Double): Boolean = {
    val aux1 = stop.tail :+ stop.head
    val aux2 = aux1.tail :+ aux1.head
    val triplets = stop.map(x => x._1.toPoint).zip(aux1.map(x => x._1.toPoint)).zip(aux2.map(x => x._1.toPoint)).map(x => (x._1._1, x._1._2, x._2)).take(stop.length - 2)
    val angles = triplets.map(x => {
      val dis1 = util.haversine(x._1, x._2)
      val dis2 = util.haversine(x._2, x._3)
      val dis3 = util.haversine(x._1, x._3)
      val angle = 180 - Math.acos((Math.pow(dis1, 2) + Math.pow(dis2, 2) - Math.pow(dis3, 2)) / (2 * dis1 * dis2)).toDegrees
      angle
    })
    val judge = angles.filter(z => z > dcc_ap)
    if (judge.length.toDouble / angles.length > pct_ap) true
    else false
  }
}