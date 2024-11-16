package consistency

import scala.collection.mutable
import org.apache.commons.math3.linear.ArrayRealVector
import kit._

@SerialVersionUID(1L)
class coarseSegment(var pointList: Array[(Double, Double)], val features: Array[Double], val tid: Int) extends Serializable {
  var len = 0.0

  def add(point: (Double, Double)): Unit = {
    val p = pointList.last
    pointList :+= point
    len += isTO.euclideanDistance(p, point)
  }

  def update(state: Array[Double]): Unit = {
    if (state(0) > features(0)) features(0) = state(0)
    if (state(1) < features(1)) features(1) = state(1)
    if (state(2) > features(2)) features(2) = state(2)
    if (state(3) > features(3)) features(3) = state(3)
  }
}

@SerialVersionUID(1L)
class fineSegment(val pointList: Array[(Double, Double)], val tid: Int) extends Serializable {
  val len: Double = isTO.euclideanDistance(pointList(0), pointList(1))

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: fineSegment => (this.pointList sameElements other.pointList) && this.tid == other.tid
      case _ => false
    }
  }

  override def hashCode(): Int = {
    val prime = 31
    val result = prime * tid + pointList.map { case (x, y) =>
      val xHash = java.lang.Double.hashCode(x)
      val yHash = java.lang.Double.hashCode(y)
      prime * xHash + yHash
    }.sum
    result
  }
}

object isTO {
  def createCoarseSegments(traj: Array[(Double, Double)], tid: Int): Array[coarseSegment] = {
    var startIndex = 0
    var length = 1
    var coarseSeg = new coarseSegment(Array(traj(startIndex)), Array(0.0, Double.PositiveInfinity, 0.0, 0.0), tid)
    var res = Array[coarseSegment]()
    while (startIndex + length < traj.length) {
      val params = Array(0.0, Double.PositiveInfinity, 0.0, 0.0)
      val currIndex = startIndex + length
      val costNoPar = MDL_nopar(traj, startIndex, currIndex)
      val costPar = MDL_par(traj, startIndex, currIndex, params)
      if (costPar > costNoPar) {
        res :+= coarseSeg
        startIndex = currIndex - 1
        length = 1
        coarseSeg = new coarseSegment(Array(traj(startIndex)), Array(0.0, Double.PositiveInfinity, 0.0, 0.0), tid)
      } else {
        length += 1
        coarseSeg.add(traj(currIndex))
        coarseSeg.update(params)
      }
    }
    coarseSeg.add(traj(traj.length - 1))
    res :+= coarseSeg
    res
  }

  def calculateCL(coarseSeg1: coarseSegment, coarseSeg2: coarseSegment, D: Int, w_per: Int, w_pral: Int, w_ang: Int): mutable.Map[fineSegment, Array[fineSegment]] = {
    val (lb, ub) = calculateLbAndUb(coarseSeg1, coarseSeg2, w_per, w_pral, w_ang)
    if (lb <= D) {
      val mp: mutable.Map[fineSegment, Array[fineSegment]] = mutable.Map()
      val lis = createFineSegments(coarseSeg1)
      val ljs = createFineSegments(coarseSeg2)
      for (k <- lis ++ ljs) { mp(k) = Array() }
      if (ub <= D) {
        for (l_i <- lis) {
          for (l_j <- ljs) {
            mp(l_i) :+= l_j
            mp(l_j) :+= l_i
          }
        }
      } else {
        for (l_i <- lis) {
          for (l_j <- ljs) {
            if (dist(l_i.pointList(0), l_i.pointList(1), l_j.pointList(0), l_j.pointList(1), w_per, w_pral, w_ang) <= D ) {
              mp(l_i) :+= l_j
              mp(l_j) :+= l_i
            }
          }
        }
      }
      return mp
    }
    mutable.Map()
  }

  def fineSegmentOutlier(distance: Double, CL: Array[fineSegment], tsum: Int, P: Double): Boolean = {
    val CTR = CL.groupBy(_.tid).count(_._2.map(_.len).sum > distance)
    math.log(CTR) <= math.ceil((1 - P) * tsum) // simulate adjustment ratio
  }

  def grid(point: GPS, bound: Box): (Double, Double) = {
    val x = (point.getLon - bound.left_bound) / (bound.right_bound - bound.left_bound) * 10000
    val y = (point.getLat - bound.lower_bound) / (bound.upper_bound - bound.lower_bound) * 10000
    (x, y)
  }

  def dist(p1: (Double, Double), p2: (Double, Double), l_s: (Double, Double), l_e: (Double, Double), w_per: Int, w_pral: Int, w_ang: Int): Double = {
    val dPer = perpendicularDis(p1, p2, l_s, l_e)._2
    val dPara = parallelDis(p1, p2, l_s, l_e)
    val dAng = angleDis(p1, p2, l_s, l_e)._2
    w_per * dPer + w_pral * dPara + w_ang * dAng
  }

  private def calculateLbAndUb(L_i: coarseSegment, L_j: coarseSegment, w_per: Int, w_pral: Int, w_ang: Int): (Double, Double) = {
    val l_per_1 = l_per(L_i.pointList.head, L_j.pointList.head, L_j.pointList.last)
    val l_per_2 = l_per(L_i.pointList.last, L_j.pointList.head, L_j.pointList.last)
    val dPerL = math.min(l_per_1, l_per_2) - (L_i.features(3) + L_j.features(3))
    val dPerU = math.max(l_per_1, l_per_2) + (L_i.features(3) + L_j.features(3))

    val b1 = within(L_i.pointList.head, L_j.pointList.head, L_j.pointList.last)
    val b2 = within(L_i.pointList.last, L_j.pointList.head, L_j.pointList.last)
    val l_para_1 = l_pral(L_i.pointList.head, L_j.pointList.head, L_j.pointList.last)
    val l_para_2 = l_pral(L_i.pointList.last, L_j.pointList.head, L_j.pointList.last)
    val dParaL = if (b1 || b2) {
      0
    } else {
      math.min(l_para_1, l_para_2)
    }
    val dParaU = if (b1 && b2) {
      math.max(L_i.len, L_j.len)
    } else if (!b1 && !b2) {
      L_i.len + L_j.len + math.min(l_para_1, l_para_2)
    } else {
      L_i.len + L_j.len - math.min(l_para_1, l_para_2)
    }

    val theta = angleDis(L_i.pointList.head, L_i.pointList.last, L_j.pointList.head, L_j.pointList.last)._1
    val dAngL = math.min(L_i.features(1), L_j.features(1)) * math.sin(theta - L_i.features(2) - L_j.features(2))
    val dAngU = math.min(L_i.features(0), L_j.features(0)) * math.sin(theta + L_i.features(2) + L_j.features(2))

    (w_per * dPerL + w_pral * dParaL + w_ang * dAngL,
      w_per * dPerU + w_pral * dParaU + w_ang * dAngU)
  }

  def euclideanDistance(a: (Double, Double), b: (Double, Double)): Double = {
    math.sqrt(math.pow(a._1 - b._1, 2) + math.pow(a._2 - b._2, 2))
  }

  private def log_2(x: Double): Double = {
    if (x == 0) return 0
    math.log(x) / math.log(2)
  }

  private def createFineSegments(coarseSeg: coarseSegment): Array[fineSegment] = {
    var finePartitions: Array[fineSegment] = Array()
    for (i <- 0 until coarseSeg.pointList.length - 1) {
      finePartitions :+= new fineSegment(Array(coarseSeg.pointList(i), coarseSeg.pointList(i + 1)), coarseSeg.tid)
    }
    finePartitions
  }

  private def MDL_nopar(traj: Array[(Double, Double)], startIndex: Int, endIndex: Int): Double = {
    val ed = euclideanDistance(traj(startIndex), traj(endIndex))
    log_2(ed)
  }

  private def MDL_par(traj: Array[(Double, Double)], startIndex: Int, endIndex: Int, params: Array[Double]): Double = {
    val L_H = MDL_nopar(traj, startIndex, endIndex)
    val L_D_H = traj.slice(startIndex, endIndex + 1)
      .sliding(2)
      .map { case Array(p1, p2) =>
        val segLen = euclideanDistance(p1, p2)
        val (maxL, dPer) = perpendicularDis(p1, p2, traj(startIndex), traj(endIndex))
        val (theta, dAng) = angleDis(p1, p2, traj(startIndex), traj(endIndex))
        if(params(0) < segLen) params(0) = segLen
        if(params(1) > segLen) params(1) = segLen
        if(params(2) < theta) params(2) = theta
        if(params(3) < maxL) params(3) = maxL
        log_2(dPer) + log_2(dAng)
      }
      .sum
    L_H + L_D_H
  }

  private def l_per(p: (Double, Double), l_s: (Double, Double), l_e: (Double, Double)): Double = {
    val v1 = new ArrayRealVector(Array(l_e._1 - l_s._1, l_e._2 - l_s._2))
    val v2 = new ArrayRealVector(Array(p._1 - l_s._1, p._2 - l_s._2))
    if (v1.getEntry(0) == 0 && v1.getEntry(1) == 0) return 0
    val t1 = v2.projection(v1).toArray
    val t2 = v2.toArray
    euclideanDistance((t1(0), t1(1)), (t2(0), t2(1)))
  }

  private def l_pral(p: (Double, Double), l_s: (Double, Double), l_e: (Double, Double)): Double = {
    val v1 = new ArrayRealVector(Array(l_e._1 - l_s._1, l_e._2 - l_s._2))
    val v2 = new ArrayRealVector(Array(p._1 - l_s._1, p._2 - l_s._2))
    if (v1.getEntry(0) == 0 && v1.getEntry(1) == 0) return euclideanDistance((v2.getEntry(0), v2.getEntry(1)), (0, 0))
    val t1 = v2.projection(v1).toArray
    val t2 = v1.toArray
    math.min(euclideanDistance((t1(0), t1(1)), (0, 0)), euclideanDistance((t1(0), t1(1)), (t2(0), t2(1))))
  }

  private def within(p: (Double, Double), l_s: (Double, Double), l_e: (Double, Double)): Boolean = {
    val v1 = new ArrayRealVector(Array(l_e._1 - l_s._1, l_e._2 - l_s._2))
    val v2 = new ArrayRealVector(Array(p._1 - l_s._1, p._2 - l_s._2))
    if (v1.getEntry(0) == 0 && v1.getEntry(1) == 0) return false
    val t1 = v2.projection(v1)
    t1.getEntry(0) * v1.getEntry(0) >= 0
  }

  private def parallelDis(p1: (Double, Double), p2: (Double, Double), l_s: (Double, Double), l_e: (Double, Double)): Double = {
    val l1 = l_pral(p1, l_s, l_e)
    val l2 = l_pral(p2, l_s, l_e)
    math.min(l1, l2)
  }

  private def perpendicularDis(p1: (Double, Double), p2: (Double, Double), l_s: (Double, Double), l_e: (Double, Double)): (Double, Double) = {
    val l1 = l_per(p1, l_s, l_e)
    val l2 = l_per(p2, l_s, l_e)
    if (l1 == 0 && l2 == 0) return (0, 0)
    (math.max(l1, l2), (math.pow(l1, 2) + math.pow(l2, 2)) / (l1 + l2))
  }

  private def angleDis(p1: (Double, Double), p2: (Double, Double), l_s: (Double, Double), l_e: (Double, Double)): (Double, Double) = {
    val v1 = new ArrayRealVector(Array(p2._1 - p1._1, p2._2 - p1._2))
    val v2 = new ArrayRealVector(Array(l_e._1 -l_s._1, l_e._2 - l_s._2))
    val a = v1.dotProduct(v2)
    val b = v1.getNorm * v2.getNorm
    var cosine_theta = a / b
    if (cosine_theta > 1) cosine_theta = 1
    val sin_theta = math.sqrt(1 - cosine_theta * cosine_theta)
    (math.asin(sin_theta), v1.getNorm * sin_theta)
  }
}