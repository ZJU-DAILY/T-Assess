package validity

import kit._

object isOrderConstraint {
  def eval(trajectory: Array[(GPS, Triplet)]): Boolean = {
    val flag = trajectory
      .map(x => (x._2.GMT, true))
      .reduce((a, b) => {
        if (a._1 > b._1) {
          (b._1, false)
        } else {
          (b._1, a._2 && b._2)
        }
      })._2
    flag
  }
}
