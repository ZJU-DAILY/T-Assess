package kit

class SegmentTree(timespan: TimeSpan, segments: Array[(MemoryBPlusTree[(Long, Long), Any], TimeSpan)]) extends Serializable {

  case class SegmentTreeNode(start: Long, end: Long, var intervals: Set[Int], left: SegmentTreeNode = null, right: SegmentTreeNode = null)

  val root: SegmentTreeNode = {
    var temp = buildSegmentTree(timespan.start / 1000, timespan.end / 1000) // second interval

    for (i <- segments.indices) {
      val ts = segments(i)._2
      temp = update(temp, ts.start / 1000, i)
      temp = update(temp, ts.end / 1000, i)
    }

    temp
  }

  private def buildSegmentTree(start: Long, end: Long): SegmentTreeNode = {
    if (start == end) SegmentTreeNode(start, end, Set())
    else {
      val mid = (start + end) / 2
      val leftChild = buildSegmentTree(start, mid)
      val rightChild = buildSegmentTree(mid + 1, end)
      SegmentTreeNode(start, end, Set(), leftChild, rightChild)
    }
  }

  private def update(node: SegmentTreeNode, pos: Long, value: Int): SegmentTreeNode = {
    if (node.start == node.end) node.copy(intervals = node.intervals ++ Array(value))
    else {
      val mid = (node.start + node.end) / 2
      if (pos <= mid) {
        val newLeft = update(node.left, pos, value)
        node.copy(left = newLeft, intervals = newLeft.intervals ++ node.right.intervals)
      } else {
        val newRight = update(node.right, pos, value)
        node.copy(right = newRight, intervals = node.left.intervals ++ newRight.intervals)
      }
    }
  }

  def query(start: Long, end: Long): Set[Int] = {
    query(root, start, end)
  }

  private def query(node: SegmentTreeNode, left: Long, right: Long): Set[Int] = {
    if (right < node.start || left > node.end) Set()
    else if (left <= node.start && right >= node.end) node.intervals
    else {
      val common = node.left.intervals.intersect(node.right.intervals)
      common ++ query(node.left, left, right) ++ query(node.right, left, right)
    }
  }
}
