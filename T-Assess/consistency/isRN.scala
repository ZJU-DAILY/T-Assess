package consistency

import scala.jdk.CollectionConverters._
import com.bmwcarit.barefoot.matcher.MatcherSample
import com.bmwcarit.barefoot.matcher.MatcherKState
import kit._

object isRN {
  private def toGeoJson(traj: Array[(GPS, Triplet)], matched: MatcherKState): (String, String) = {
    var o1: String = "["
    traj.foreach(x => {
      o1 += "[" + x._1.getLon + "," + x._1.getLat + "],"
    })
    o1 += "]"

    var o2: String = "["
    matched.sequence().asScala.foreach(x => {
      val p = x.point().geometry()
      o2 += "[" + p.getX + "," + p.getY + "],"
    })
    o2 += "]"

    (o1, o2)
  }

  def evalOff(traj: Iterable[(GPS, Triplet)], bm: BroadcastMatcher): Iterable[Long] = {
    // Load trace data (object-id: String, time: Long, position: Point, point-id: Long)
    val traces = traj.map(x => (x._2.tid.toString, util.string2timestamp(x._2.GMT), x._1.toPoint, x._2.pid))
    // Run a map job on RDD that uses the matcher instance
    val trip = traces.map(x => new MatcherSample(x._1, x._2, x._3))
    val matched = bm.mmatch(trip, minDistance = 0, minInterval = 0)
    if (matched.size() == 0) {
      return traj.map(_._2.pid)
    } else if (matched.sequence().size() != traj.size) {
      // part of the trajectory can't be matched
      val ts1 = traces.map(x => (x._2 / 1000, x._4))
      val ts2 = matched.samples().asScala.map(_.time()).toSet
      val unmatched = ts1.filter {case (time, _) => !ts2.contains(time)}
      return unmatched.map(_._2)
    }
    Array()
  }

  // online
  def evalOn(traj: Iterable[(GPS, Triplet)], mks: MatcherKState, bm: BroadcastMatcher): (Array[Long], MatcherKState) = {
    val trace = traj.map(x => (x._2.tid.toString, util.string2timestamp(x._2.GMT), x._1.toPoint, x._2.pid))
    val samples = trace.map(x => (new MatcherSample(x._1, x._2, x._3), x._4))
    bm.tmatch(samples, mks)
  }
}