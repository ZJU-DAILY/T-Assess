package completeness

import kit._

object hasMP {
  def eval(traj: Iterable[(GPS, Triplet)], token: Boolean, m: Int = 0): (Long, Iterable[Int]) = {
    if (traj.size > 1) {
      val aux = traj.toArray.tail :+ traj.head
      val intervals = traj.zip(aux).map(x => (x._1._2.GMT, x._2._2.GMT))
        .map(y => math.max(0, (util.string2timestamp(y._2) - util.string2timestamp(y._1)) / 1000).toInt)
        .take(traj.size - 1)
      val mode = if (token || m == 0) {
        intervals.groupBy(x => x).map(y => (y._1, y._2.size))
          .reduce((a, b) => {
            if (a._2 > b._2 || (a._2 == b._2 && a._1 > b._1)) a
            else b
          })._1
      } else {
        m
      }
      if (mode == 0) return (0, intervals)
      val cnt = intervals.fold(0)((a, b) => {
        if (b / mode == 0) a
        else a + b / mode - 1
      })
      assert(cnt >= 0)
      return (cnt, intervals)
    }
    (0, Iterable())
  }
}