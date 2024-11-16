package evaluation

import scala.collection.mutable
import com.bmwcarit.barefoot.matcher.MatcherKState
import kit._

object variables {
  var num_partition = 0 // spark partition number
  var t_num = 0 // dataset trajectory numbers
  var p_num = 0L // dataset trajectory point number
  var map_matcher: BroadcastMatcher = _ // isRN map matching tool
  // isPSM parameters
  val σ = 4.0
  val σ_s = 6.23
  val deviation = 100
  // isSC parameters
  val eps = 50 // geolife_bike-20 ais-50 rome/tdrive-50
  val min_time = 300 // geolife_bike-60 ais-300 rome/tdrive-180
  val slow_point_thresh = 10 // geolife_bike-2, ais-10 rome/tdrive-2
  val cluster_eps = 100 // geolife_bike-100
  val min_pts = 50 // evaluate online use
  val pct_ap = 0.6 // evaluate online use
  val dcc_ap = 0.8 // evaluate online use
  // isTO parameters
  val D = 100 // A larger value of D generates a smaller number of outliers, and a smaller value of D generates a larger number of outliers (geolife_bike-100)
  val P = 0.95 // p = 0.95 may suffice when |I| < 10^3, but p = 0.99 may be more appropriate when |I| ≈ 10^6
  val F = 0.1 // F = 0.2 may suffice when avg(len_i) < 100, but F = 0.1 may be more appropriate when avg(len_i) > 1000
  val w_per = 1
  val w_pral = 1
  val w_ang = 1
  // fairness index parameters
  val cap = 5000 // maximum number of points in leaf node in quad-tree original (geolife_bike-5000)
  val order = 5 // the branching factor (maximum number of branches in an internal node) of the B+-tree
  var geo_bound: Box = _ // spatial span of quad-tree
  var time_bound: TimeSpan = _ // temporal span of quad-tree
  var timespan = Array[TimeSpan]()
  var boxs = Array[Box]()
  // validity statistics
  val isInRange_tids = mutable.Set[Int]()
  val isOrderConstraint_tids = mutable.Set[Int]()
  // completeness statistics
  val hasMP_tids = mutable.Set[Int]()
  var hasMP_pcnt = 0L
  // consistency statistics
  val isTLC_tids = mutable.Set[Int]()
  val isRN_tids = mutable.Set[Int]()
  val isRN_pids = mutable.Set[Long]()
  val isSC_tids = mutable.Set[Int]()
  val isSC_pids = mutable.Set[Long]()
  val isPSM_tids = mutable.Set[Int]()
  val isPSM_pids = mutable.Set[Long]()
  val isTO_tids = mutable.Set[Int]()
  // fairness statistics
  val isSpatialUniform_density = mutable.Map[Box, Double]()
  val isTemporalUniform_density = mutable.Map[TimeSpan, Double]()


  // online
  val time_window = 60000 // ms
  var delays = Array[Double]()
  var window_point_cnt = Array[Long]()
  var validity_cost = Array[Double]()
  var completeness_cost = Array[Double]()
  var consistency_cost = Array[Double]()
  var fairness_cost = Array[Double]()

  val heaps = mutable.Map[Int, mutable.PriorityQueue[(Int, Int)]]() // hasMP
  val lens = mutable.Map[Int, Double]() // isTLC
  val matcherKStates = mutable.Map[Int, MatcherKState]() // isRN
  val stop_groups = mutable.Map[Int, Array[(GPS, Triplet)]]() // isSC
  var ps = Array[Array[(GPS, Triplet)]]()
  var quad_tree_onlineEval: QuadTree = _
  var bplus_tree_onlineEval: Array[(MemoryBPlusTree[(Long, Long), Any], TimeSpan)] = _
}
