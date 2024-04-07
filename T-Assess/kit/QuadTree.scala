package kit

/**
 * The Class representing a complete QuadTree, most operations are implemented
 * inside the inner Node class and called recursively, The outer methods just call
 * the Node implementations with root as the node.
 *
 * @param K Maximum number of elements that can fit inside a leaf node
 * @param geoBound span in geographic space
 */
class QuadTree(K: Int, geoBound: Box) extends Serializable {
  private case class Element(position: GPS, data: Triplet)

  private val root = new Node(K, geoBound)

  // Public interface callers, Work is done mostly inside Node Class
  def build(elements: Iterable[(GPS, Triplet)]): Unit = elements.foreach(e => insert(e._1, e._2))

  def rangeSearch(queryBox: Box): Array[(GPS, Triplet)] = root.rangeSearch(queryBox).map(Element.unapply(_).get)

  def dfsBPlusTree(order: Int): Array[(MemoryBPlusTree[(Long, Long), Any], TimeSpan)] = root.dfsBPlusTree(order)

  private def insert(p: GPS, data: Triplet): Boolean = root.insert(Element(p, data))

  private class Node(K: Int, var bounds: Box) extends Serializable {
    private var elements: Array[Element] = Array()

    private var topLeft: Node = _
    private var topRight: Node = _
    private var bottomLeft: Node = _
    private var bottomRight: Node = _

    private def children: Array[Node] = Array(topLeft, topRight, bottomLeft, bottomRight) // For iterative access
    private def isLeaf: Boolean = topLeft == null

    /**
     * Inserts the element into the subtree of the node called
     *
     * Recursively performs insertion to the QuadTree by
     * following the correct children nodes. Once a leaf is reached
     * the Element is inserted. If the number of elements of a leaf node
     * is more than K, then a split operation is performed, creating 4 more
     * child nodes and moving the leaf's elements to them.
     *
     * @param elem The Element object to be inserted
     * @return true if insertion took place or false if not
     */
    def insert(elem: Element): Boolean = {
      def split(): Unit = {
        topLeft = new Node(K, bounds.topLeftSubBox)
        bottomLeft = new Node(K, bounds.bottomLeftSubBox)
        topRight = new Node(K, bounds.topRightSubBox)
        bottomRight = new Node(K, bounds.bottomRightSubBox)

        elements.foreach(e => findSubtree(e.position).insert(e))
        elements = Array()
      }

      def insertElement(): Boolean = {
        var returnValue = false  //Returns true if value actually inserted
        if (!elements.exists(_.data.poi_id == elem.data.poi_id)) {
          elements :+= elem
          returnValue = true
        }
        if (elements.length > K) split()
        returnValue
      }

      if (!bounds.contain(elem.position)) {
        throw new IllegalArgumentException(s"The position '${elem.position}' is out of QuadTree bounds.")
      }

      this.synchronized {
        if (isLeaf) insertElement()
        else findSubtree(elem.position).insert(elem)
      }
    }

    /**
     * Return the child node which bounds, contain the position point
     *
     * @param p Position
     * @return The child node
     */
    private def findSubtree(p: GPS): Node = {
      // Potential no such element exception
      try {
        children.find(_.bounds.contain(p)).get
      }
      catch {
        case _: Exception => throw new Exception("Double precision overflow.")
      }
    }

    /**
     * Performs a range search on the Quadtree
     *
     * Recursively calls rangeSearch to all the nodes which overlap
     * with the boundary formed by the range bounds specified.
     * When a leaf is reached, it returns a list of the elements inside the
     * range. While the recursion unravels, the lists as concatenated returning
     * a full list of all the elements in the range inside the tree.
     *
     * @param queryBox The range search box
     * @return A list of elements inside the specified range.
     */
    def rangeSearch(queryBox: Box): Array[Element] = {
      if (!(queryBox.bottomLeftPoint <= queryBox.topRightPoint)) return Array()

      if (isLeaf)
        elements.filter(e => e.position >= queryBox.bottomLeftPoint && e.position <= queryBox.topRightPoint)
      else
        children.filter(_.bounds.overlap(queryBox)).foldLeft(Array[Element]()) {_ ++ _.rangeSearch(queryBox)}
    }

    /**
     * Visits all tree nodes in DFS order, create a B+-tree and get timespan for every node
     */
    def dfsBPlusTree(order: Int): Array[(MemoryBPlusTree[(Long, Long), Any], TimeSpan)] = {
      if (isLeaf) {
        val index = new MemoryBPlusTree[(Long, Long), Any](order)
        if (elements.nonEmpty) {
          for (ele <- elements) {
            index.insert((util.string2timestamp(ele.data.GMT), ele.data.poi_id))
          }
          Array((index, TimeSpan(index.minKey.get._1, index.maxKey.get._1)))
        } else {
          Array()
        }
      } else {
        children.foldLeft(Array[(MemoryBPlusTree[(Long, Long), Any], TimeSpan)]()) {_ ++ _.dfsBPlusTree(order)}
      }
    }
  } // End Node class
} // End QuadTree class
