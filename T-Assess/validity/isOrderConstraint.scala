package validity

import kit._

object isOrderConstraint {
  def eval(traj: Iterable[(GPS, Triplet)]): Boolean = {
    if (traj.size > 1) {
      val flag = traj.map(x => (x._2.GMT, true))
        .reduce((a, b) => {
          if (a._1 > b._1) {
            (b._1, false)
          } else {
            (b._1, a._2 && b._2)
          }
        })._2
      return !flag
    }
    false
  }
}