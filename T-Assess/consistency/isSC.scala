package consistency

import scala.util.control.Breaks._
import scala.collection.mutable
import scala.annotation.tailrec
import com.esri.core.geometry.Point
import kit._
import evaluation._

object isSC {
  // offline
  def detectStopWithGeoOff(trajectory: Array[(GPS, Triplet)]): mutable.ListBuffer[Array[(GPS, Triplet)]] = {
    val ps = mutable.ListBuffer[Array[(GPS, Triplet)]]()
    var i = 0
    val num = trajectory.length
    while (i < num) {
      var j = i + 1
      var token = 0
      breakable {
        while (j < num) {
          val dis = util.haversine(trajectory(i)._1.toPoint, trajectory(j)._1.toPoint)
          if (dis > evaluate.eps) {
            val interval = (util.string2timestamp(trajectory(j - 1)._2.GMT) - util.string2timestamp(trajectory(i)._2.GMT)) / 1000
            if (interval > evaluate.min_time) {
              ps.append(trajectory.slice(i, j))
              i = j
              token = 1
            }
            break
          }
          j += 1
        }
      }
      if (token != 1) i += 1
    }
    ps
  }

  // online
  def initState(ids: Array[Int]): Unit = {
    for (id <- ids) { evaluate.groups.put(id, Array()) }
  }

  private def updateGroup(group: Array[(GPS, Triplet)]): (Int, mutable.ListBuffer[Array[(GPS, Triplet)]]) = {
    val ps = mutable.ListBuffer[Array[(GPS, Triplet)]]()
    var i = 0
    val num = group.length
    while (i < num) {
      var j = i + 1
      if (j == num) return (i, ps)
      var token = 0
      breakable {
        while (j < num) {
          val dis = util.haversine(group(i)._1.toPoint, group(j)._1.toPoint)
          if (dis > evaluate.eps) {
            val interval = (util.string2timestamp(group(j - 1)._2.GMT) - util.string2timestamp(group(i)._2.GMT)) / 1000
            if (interval > evaluate.min_time) {
              ps.append(group.slice(i, j))
              i = j
              token = 1
            }
            break
          }
          j += 1
          if (j == num) return (i, ps)
        }
      }
      if (token != 1) i += 1
    }
    (-1, ps)
  }

  def detectStopWithGeoOn(trajectory: Array[(GPS, Triplet)], g: Array[(GPS, Triplet)]): (mutable.ListBuffer[Array[(GPS, Triplet)]], Array[(GPS, Triplet)]) = {
    val ps = mutable.ListBuffer[Array[(GPS, Triplet)]]()
    var group = if (g.nonEmpty) g else Array(trajectory.head)
    val index = if (g.nonEmpty) 0 else 1
    for (i <- index until trajectory.length) {
      val dis = util.haversine(group.head._1.toPoint, trajectory(i)._1.toPoint)
      if (dis <= evaluate.eps) group :+= trajectory(i)
      else {
        val interval = (util.string2timestamp(group.last._2.GMT) - util.string2timestamp(group.head._2.GMT)) / 1000
        if (interval > evaluate.min_time) {
          group = Array(trajectory(i))
          ps.append(group)
        }
        else {
          group = group.drop(1) :+ trajectory(i)
          val res = updateGroup(group)
          assert(res._1 != -1)
          group = group.drop(res._1)
        }
      }
    }
    (ps, group)
  }

  // shared functions
  def calculatePCT(stop: Array[(GPS, Triplet)]): Boolean = {
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
    val judge = angles.filter(z => z > evaluate.dcc_ap)
    if (judge.length.toDouble / angles.length > evaluate.pct_ap) true
    else false
  }

  def centerPoint(points: Array[(GPS, Triplet)]): Point = {
    val clon = points.map(x => x._1.getLon).sum / points.length
    val clat = points.map(x => x._1.getLat).sum / points.length
    new Point(clon, clat)
  }

  private def neighbours(stop: (Array[(GPS, Triplet)], Point), stops: Array[(Array[(GPS, Triplet)], Point)], dis: Double): mutable.Queue[(Array[(GPS, Triplet)], Point)] = {
    val temp = stops.map(x => (x, util.haversine(x._2, stop._2) < dis)).filter(_._2).map(y => y._1)
    val N = mutable.Queue[(Array[(GPS, Triplet)], Point)]()
    N.enqueueAll(temp)
    N
  }

  @tailrec
  def hierarchyDiscover(stop: Array[(Array[(GPS, Triplet)], Point)], level: Int): Unit = {
    var res = Array[(Array[(GPS, Triplet)], Point)]()
    val dis = evaluate.gran(level - 1)
    val mp = mutable.Map.empty[(Array[(GPS, Triplet)], Point), Boolean] ++ stop.map(x => (x, false)).toMap
    for (si <- stop) {
      if (!mp(si)) {
        mp(si) = true
        val N = neighbours(si, stop, dis)
        var cluster = N.toArray
        while (N.nonEmpty) {
          val sj = N.dequeue()
          if (!mp(sj)) {
            mp(sj) = true
            val M = neighbours(sj, stop, dis)
            N.addAll(M)
            cluster :++= M
          }
        }
        val new_stop = cluster.distinct.flatMap(x => x._1)
        res :+= (new_stop, centerPoint(new_stop))
      }
    }
    evaluate.hierarchy_stops.append(res)
//    println(" *** level " + level +": " + evaluate.hierarchy_stops(level).length + " stops")
    if (level != evaluate.height) hierarchyDiscover(evaluate.hierarchy_stops(level), level + 1)
  }
}
