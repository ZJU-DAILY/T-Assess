package fairness

import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import kit._

object isSpatialUniform {
  def eval(grqRDD: RDD[Box], quad_tree: Broadcast[QuadTree], qp_cnt: Long, geo_bound: Box): Array[(Box, Double)] = {
    grqRDD.map(x => (x, quad_tree.value.rangeSearch(x)))
      .map(y => (y._1, y._2.map(i => i._2.poi_id).distinct.length))
      .map(z => {
        val i = (z._2.toDouble / qp_cnt) * (geo_bound.getArea / z._1.getArea)
        val density = i / (i + 1)
        (z._1, density)
      })
      .collect()
  }
}
