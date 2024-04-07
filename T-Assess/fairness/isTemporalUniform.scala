package fairness

import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import kit._

object isTemporalUniform {
  def eval(trqRDD: RDD[TimeSpan], segment_tree: Broadcast[SegmentTree], BPlus_trees: Broadcast[Array[(MemoryBPlusTree[(Long, Long), Any], TimeSpan)]], qp_cnt: Long, dst_bound: TimeSpan): Array[(TimeSpan, Double)] = {
    trqRDD.map(x => {
        val related = segment_tree.value.query(x.start / 1000, x.end / 1000)
        val res = related.flatMap(y => {BPlus_trees.value(y)._1.boundedKeysIterator((Symbol(">="), (x.start, -1)), (Symbol("<"), (x.end + 1, -1)))})
        val i = (res.size.toDouble / qp_cnt) * (dst_bound.getInterval.toDouble / x.getInterval)
        val density = i / (i + 1)
        (x, density)
      })
      .collect()
  }
}
