package validity

import kit._

object isInRange {
  def eval(traj: Iterable[(GPS, Triplet)]): Boolean = {
    for (i <- traj) {
      try {
        util.string2timestamp(i._2.GMT)
        if (math.abs(i._1.getLon) > 180 || math.abs(i._1.getLat) > 90) throw new Exception()
      } catch {
        case _: Exception => return true
      }
    }
    false
  }
}