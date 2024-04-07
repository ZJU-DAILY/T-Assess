package validity

import kit._

object isOutOfBound {
  def eval(record: (GPS, Triplet), geo_bound: Box): Boolean = {
    if (!geo_bound.contain(record._1)) {
      return false
    }
    true
  }
}
