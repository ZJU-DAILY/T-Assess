package kit

import java.util.ArrayList
import scala.jdk.CollectionConverters._
import scala.collection.mutable
import com.bmwcarit.barefoot.matcher.{Matcher, MatcherKState, MatcherSample}
import com.bmwcarit.barefoot.roadmap.{Road, RoadMap, RoadPoint, TimePriority}
import com.bmwcarit.barefoot.spatial.Geography
import com.bmwcarit.barefoot.topology.Dijkstra

object BroadcastMatcher {
  private var instance: Matcher = _
  val MatcherKStates = mutable.Map[Long, MatcherKState]() // Create initial (empty) state memory

  private def initialize(uri: String): Unit = {
    if (instance != null) return
    this.synchronized {
      if (instance == null) {
        // initialize map matcher once per Executor (JVM process/cluster node)
        val map = RoadMap.Load(new LocalMapReader(uri)).construct()
        val router = new Dijkstra[Road, RoadPoint]()
        val cost = new TimePriority()
        val spatial = new Geography()
        instance = new Matcher(map, router, cost, spatial)
      }
    }
  }

  def initState(ids: Array[Int]): Unit = {
    for (id <- ids) { BroadcastMatcher.MatcherKStates.put(id, new MatcherKState()) }
  }
}

@SerialVersionUID(1L)
class BroadcastMatcher(uri: String) extends Serializable {
  // offline matcher
  def mmatch(samples: Array[MatcherSample], minDistance: Double, minInterval: Int): MatcherKState = {
    BroadcastMatcher.initialize(uri)
    BroadcastMatcher.instance.mmatch(new ArrayList[MatcherSample](samples.toList.asJava), minDistance, minInterval)
  }

  // online matcher
  def tmatch(id: Long, samples: Array[(MatcherSample, Long)], mks: MatcherKState): (Array[Long], MatcherKState) = {
    BroadcastMatcher.initialize(uri)
    var unmatched = Array[Long]()
    // Iterate over sequence (stream) of samples
    for (sample <- samples) {
      // Execute matcher with single sample and update state memory
      mks.update(BroadcastMatcher.instance.execute(mks.vector, mks.sample, sample._1), sample._1)
      // Access map matching result: estimate for most recent sample
      val estimate = mks.estimate()
      if (estimate == null) {
        unmatched :+= sample._2
      }
    }
    (unmatched, mks)
  }
}
