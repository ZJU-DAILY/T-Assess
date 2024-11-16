package consistency

import kit._

object isTLC {
  def eval(traj: Iterable[(GPS, Triplet)]): Double = {
    if (traj.size > 1) {
      val len = traj.sliding(2).collect {
        case Seq(a, b) =>
          val d = util.haversine(a._1.toPoint, b._1.toPoint)
          if (d.isNaN) 0 else d
      }.sum
      return len
    }
    0
  }
}