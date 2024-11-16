package consistency

import org.apache.commons.math3.filter.{DefaultMeasurementModel, DefaultProcessModel, KalmanFilter}
import org.apache.commons.math3.linear.{Array2DRowRealMatrix, ArrayRealVector, RealMatrix, RealVector}
import kit._

object isPSM {
  def kalmanFilter(traj: Iterable[(GPS, Triplet)], σ: Double, σ_s: Double, deviation: Int): Iterable[Long] = {
    val H = new Array2DRowRealMatrix(Array(
      Array(1.0, 0, 0, 0),
      Array(0, 1.0, 0, 0)
    )) // measurement matrix
    val R = new Array2DRowRealMatrix(Array(
      Array(σ * σ, 0),
      Array(0, σ * σ)
    )) // measurement noise covariance matrix
    var P0: RealMatrix = new Array2DRowRealMatrix(Array(
      Array(σ_s * σ_s, 0, 0, 0),
      Array(0, σ_s * σ_s, 0, 0),
      Array(0, 0, σ_s * σ_s, 0),
      Array(0, 0, 0, σ_s * σ_s)
    )) // initial estimate of state error covariance
    val Q = new Array2DRowRealMatrix(Array(
      Array(0.0, 0, 0, 0),
      Array(0, 0.0, 0, 0),
      Array(0, 0, σ_s * σ_s, 0),
      Array(0, 0, 0, σ_s * σ_s)
    )) // system noise covariance matrix
    var x: RealVector = new ArrayRealVector(Array(traj.head._1.getLat, traj.head._1.getLon, 0.0, 0.0)) // initial state estimate

    val res = traj.sliding(2).collect {
      case Seq(a, b) =>
        val dt = (util.string2timestamp(b._2.GMT) - util.string2timestamp(a._2.GMT)) / 1000
        val A = new Array2DRowRealMatrix(Array(
          Array(1.0, 0, dt, 0),
          Array(0, 1.0, 0, dt),
          Array(0, 0, 1.0, 0),
          Array(0, 0, 0, 1.0)
        )) // system matrix

        val pm = new DefaultProcessModel(A, null, Q, x, P0)
        val mm = new DefaultMeasurementModel(H, R)
        val filter = new KalmanFilter(pm, mm)
        filter.predict()
        val z = new ArrayRealVector(Array(a._1.getLat, a._1.getLon))
        filter.correct(z)

        x = filter.getStateEstimationVector
        P0 = filter.getErrorCovarianceMatrix

        val estimate = x.toArray
        if (util.haversine(a._1.toPoint, GPS(estimate(1), estimate(0)).toPoint) > deviation) a._2.pid else -1
    }.filter(_ >= 0).to(Iterable)
    res
  }
}