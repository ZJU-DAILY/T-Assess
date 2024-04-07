package consistency

import com.bmwcarit.barefoot.spatial.Geography
import kit._
import evaluation._

object isTLC {
  // offline
  def evalOff(trajectory: Array[(GPS, Triplet)], threshold: Double): Boolean = {
    val len = trajectory.map(x => x._1.toPoint).map(y => (y, 0.0))
      .reduce((a, b) => {
        (b._1, a._2 + util.haversine(a._1, b._1))
      })._2
    len >= threshold
  }

  // online
  def initState(ids: Array[Int]): Unit = {
    for (id <- ids) {
      evaluate.idles.put(id, 0)
      evaluate.lens.put(id, 0)
    }
  }

  def updateState(): Unit = {
    for (key <- evaluate.idles.keys) {
      evaluate.idles.update(key, evaluate.idles(key) + 1)
    }
  }

  def checkState(): Unit = {
    for (key <- evaluate.idles.keys) {
      if (evaluate.idles(key) == evaluate.wds_max) {
        if (evaluate.lens(key) < evaluate.p2 * evaluate.lens.values.sum / evaluate.lens.count(_._2 != 0)) {
          evaluate.isTLC_tids.add(key)
        }
        evaluate.idles(key) = 0
        evaluate.lens(key) = 0
      }
    }
  }

  def evalOn(trajectory: Array[(GPS, Triplet)]): Double = {
    var dis = 0.0
    for (i <- 0 until trajectory.length - 1) {
      dis += new Geography().distance(trajectory(i)._1.toPoint, trajectory(i + 1)._1.toPoint)
    }
    dis
  }
}
