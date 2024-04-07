package consistency

import scala.jdk.CollectionConverters._
import com.bmwcarit.barefoot.matcher.MatcherSample
import com.bmwcarit.barefoot.matcher.MatcherKState
import org.apache.spark.broadcast.Broadcast
import kit._

object isRN {
  // offline
  def evalOff(trajectory: Array[(GPS, Triplet)], matcher: Broadcast[BroadcastMatcher]): (Boolean, Array[Long]) = {
    // Load trace data (object-id: String, time: Long, position: Point, point-id: String)
    val traces = trajectory.map(x => (x._2.traj_id.toString, util.string2timestamp(x._2.GMT), x._1.toPoint, x._2.poi_id))
    // Run a map job on RDD that uses the matcher instance
    val trip = traces.map(x => new MatcherSample(x._1, x._2, x._3))
    val matched = matcher.value.mmatch(trip, minDistance = 0, minInterval = 0)

//    print("[")
//    trajectory.foreach(x => {
//      print("[" + x(1) + "," + x(2) + "],")
//    })
//    println("]")
//    print("[")
//    matched.sequence().asScala.foreach(x => {
//      val p = x.point().geometry()
//      print("[" + p.getX + "," + p.getY + "],")
//    })
//    println("]")

    if (matched.size() == 0) (false, trajectory.map(x => x._2.poi_id))
    else if (matched.sequence().size() != trajectory.length) {
      // part of the trajectory can't be matched
      val ts1 = traces.map(x => (x._2 / 1000, x._4))
      val ts2 = matched.samples().asScala.toArray.map(x => (x.time(), "0")).toMap
      val unmatched = ts1.map(x => (x._1, (x._2, ts2.get(x._1)))).filter(_._2._2.isEmpty).map(y => y._2._1)
      return (false, unmatched)
    }
    (true, Array())
  }

  // online
  def evalOn(trajectory: Array[(GPS, Triplet)], matcher: Broadcast[BroadcastMatcher], mks: MatcherKState): (Boolean, Array[Long], MatcherKState) = {
    val trace = trajectory.map(x => (x._2.traj_id.toString, util.string2timestamp(x._2.GMT), x._1.toPoint, x._2.poi_id))
    val samples = trace.map(x => (new MatcherSample(x._1, x._2, x._3), x._4))
    val res = matcher.value.tmatch(trajectory.head._2.traj_id, samples, mks)
    if (res._1.length != 0) return (false, res._1, res._2)
    (true, res._1, res._2)
  }
}
