package validity

import kit._

object isInRange {
  def eval(record: (GPS, Triplet)): Boolean = {
    try {
      util.string2timestamp(record._2.GMT)
      if (math.abs(record._1.getLon) > 180 || math.abs(record._1.getLat) > 90) throw new Exception()
    } catch {
      case _: Exception => return false
    }
    true
  }
}
