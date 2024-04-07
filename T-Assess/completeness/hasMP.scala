package completeness

import scala.collection.mutable
import kit._

object hasMP {
  // offline
  def evalOff(trajectory: Array[(GPS, Triplet)]): Int = {
    if (trajectory.length != 1) {
      val aux = trajectory.tail :+ trajectory.head
      val pairs = trajectory.zip(aux).map(x => (x._1._2.GMT, x._2._2.GMT))
      val intervals = pairs.map(y => ((util.string2timestamp(y._2) - util.string2timestamp(y._1)) / 1000).toInt).take(trajectory.length - 1)
      val mode = intervals.groupBy(x => x).map(y => (y._1, y._2.length))
        .reduce((a, b) => {
          if (a._2 > b._2 || (a._2 == b._2 && a._1 > b._1)) a
          else b
        })._1
//      assert(mode != 0)
      if (mode == 0) return 0
      val cnt = intervals.fold(0)((a, b) => {
        val div = b / mode
        var t = 0
        if (div == 0) t = 0
        else t = div - 1
        a + t
      })
      return cnt
    }
    0
  }

  //online
  def getIntervals(trajectory: Array[(GPS, Triplet)]): Array[Int] = {
    if (trajectory.length != 1) {
      val aux = trajectory.tail :+ trajectory.head
      val pairs = trajectory.zip(aux).map(x => (x._1._2.GMT, x._2._2.GMT))
      val intervals = pairs.map(y => ((util.string2timestamp(y._2) - util.string2timestamp(y._1)) / 1000).toInt).take(trajectory.length - 1)
      return intervals
    }
    Array()
  }

  def getCnt(id: Int, intervals: Array[Int], heaps: mutable.Map[Int, mutable.PriorityQueue[(Int, Int)]]): Int = {
    val mode = heaps(id).head._2
    assert(mode != 0)
    val cnt = intervals.fold(0)((a, b) => {
      val div = b / mode
      var t = 0
      if (div == 0) t = 0
      else t = div - 1
      a + t
    })
    cnt
  }
}
