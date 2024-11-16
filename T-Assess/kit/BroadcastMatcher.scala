package kit

import scala.jdk.CollectionConverters._
import com.bmwcarit.barefoot.matcher.{Matcher, MatcherKState, MatcherSample}
import com.bmwcarit.barefoot.roadmap.{Road, RoadMap, RoadPoint, TimePriority}
import com.bmwcarit.barefoot.spatial.Geography
import com.bmwcarit.barefoot.topology.Dijkstra

@SerialVersionUID(1L)
class BroadcastMatcher(uri: String) extends Serializable {
  @transient val instance: Matcher = {
    val map = RoadMap.Load(new LocalMapReader(uri)).construct()
    val router = new Dijkstra[Road, RoadPoint]()
    val cost = new TimePriority()
    val spatial = new Geography()
    new Matcher(map, router, cost, spatial)
  }

  // offline matcher
  def mmatch(samples: Iterable[MatcherSample], minDistance: Double, minInterval: Int): MatcherKState = {
    instance.mmatch(new java.util.ArrayList[MatcherSample](samples.toList.asJava), minDistance, minInterval)
  }

  // online matcher
  def tmatch(samples: Iterable[(MatcherSample, Long)], mks: MatcherKState): (Array[Long], MatcherKState) = {
    var unmatched = Array[Long]()
    // Iterate over sequence (stream) of samples
    for (sample <- samples) {
      // Execute matcher with single sample and update state memory
      mks.update(instance.execute(mks.vector, mks.sample, sample._1), sample._1)
      // Access map matching result: estimate for most recent sample
      val estimate = mks.estimate()
      if (estimate == null) {
        unmatched :+= sample._2
      }
    }
    (unmatched, mks)
  }
}