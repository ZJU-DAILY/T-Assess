package kit

import collection.mutable.ArrayBuffer

/**
 * An in-memory B+ Tree implementation.
 *
 * @constructor creates an in-memory B+ Tree with a branching factor of `order`.
 * @param order the branching factor (maximum number of branches in an internal node) of the tree
 * @tparam K the type of the keys contained in this map.
 * @tparam V the type of the values associated with the keys.
 */
class MemoryBPlusTree[K <% Ordered[K], V]( val order: Int ) extends BPlusTree[K, V] {
  require(order >= 3, "order must be at least 3")

  protected type N = Node[K, V]

  protected var first: Node[K, V] = new LeafNode[K, V](null)
  protected var last: Node[K, V] = first
  protected var root: Node[K, V] = first
  protected var lastlen = 0

  protected def addBranch(node: Node[K, V], branch: Node[K,V]): Unit = node.asInternal.branches += branch

  protected def addKey(node: Node[K, V], key: K): Unit = node.keys += key

  protected def freeKey(node: N, index: Int): Unit = {}

  protected def freeNode(node: Node[K, V]): Unit = {}

  protected def freeValue(node: N, index: Int): Unit = {}

  protected def getBranch(n: Node[K, V], index: Int): Node[K, V] = n.asInternal.branches(index)

  protected def getBranches(n: Node[K, V]): Seq[Node[K, V]] = n.asInternal.branches.toSeq

  protected def getKey(n: Node[K, V], index: Int): K = n.keys(index)

  protected def getKeys(node: Node[K, V]): Seq[K] = node.keys.toSeq

  protected def getNext(node: Node[K, V]): Node[K, V] = node.next

  protected def getParent(node: Node[K, V]): InternalNode[K, V] = node.parent

  protected def getPrev(node: Node[K, V]): Node[K, V] = node.prev

  protected def getValue(n: Node[K, V], index: Int): V = n.asLeaf.values(index)

  protected def getValues(node: Node[K, V]): Seq[V] = node.asLeaf.values.toSeq

  protected def insertInternal(n: Node[K, V], keyIndex: Int, key: K, branchIndex: Int, branch: Node[K, V]): Unit = {
    n.keys.insert(keyIndex, key)
    n.asInternal.branches.insert(branchIndex, branch)
  }

  protected def insertLeaf[V1 >: V](n: Node[K, V], index: Int, key: K, value: V1): Unit = {
    n.keys.insert(index, key)
    n.asInstanceOf[Node[K, V1]].asLeaf.values.insert(index, value)
  }

  protected def isLeaf(node: Node[K, V]): Boolean = node.isLeaf

  protected def nodeLength(node: Node[K, V]): Int = node.keys.length

  protected def moveInternal(src: N, begin: Int, end: Int, dst: N, index: Int): Unit = {
    dst.keys.insertAll(index, src.keys.view.slice(begin, end))
    src.keys.remove(begin, end - begin)
    dst.asInternal.branches.insertAll(index + 1, src.asInternal.branches.view.slice(begin + 1, end + 1))
    src.asInternal.branches.remove(begin + 1, end - begin)
  }

  protected def moveLeaf(src: Node[K, V], begin: Int, end: Int, dst: Node[K, V], index: Int): Unit = {
    dst.keys.insertAll(index, src.keys.view.slice(begin, end))
    src.keys.remove(begin, end - begin)
    dst.asLeaf.values.insertAll(index, src.asLeaf.values.view.slice(begin, end))
    src.asLeaf.values.remove(begin, end - begin)
  }

  protected def newInternal(parent: Node[K, V]) = new InternalNode(parent.asInstanceOf[InternalNode[K, V]])

  protected def newLeaf(parent: Node[K, V]) = new LeafNode(parent.asInstanceOf[InternalNode[K, V]])

  protected def newRoot(branch: Node[K, V]): InternalNode[K, V] = {
    val res = new InternalNode[K, V](null)
    res.branches += branch
    res
  }

  protected def nul: Null = null

  protected def removeInternal(node: Node[K, V], keyIndex: Int, branchIndex: Int): Int = {
    node.keys.remove(keyIndex, 1)
    node.asInternal.branches.remove(branchIndex, 1)
    node.length
  }

  protected def removeLeaf(node: Node[K, V], index: Int): Int = {
    node.keys.remove(index, 1)
    node.asLeaf.values.remove(index, 1)
    node.length
  }

  protected def setFirst(leaf: Node[K, V]): Unit = {}

  protected def setKey(node: Node[K, V], index: Int, key: K): Unit = node.keys(index) = key

  protected def setLast(leaf: Node[K, V]): Unit = {}

  protected def setNext(node: Node[K, V], p: Node[K, V]): Unit = node.next = p

  protected def setParent(node: Node[K, V], p: Node[K, V]): Unit = node.parent = p.asInstanceOf[InternalNode[K, V]]

  protected def setPrev(node: Node[K, V], p: Node[K, V]): Unit = node.prev = p

  protected def setRoot(node: Node[K, V]): Unit = {}

  protected def setValue[V1 >: V](node: Node[K, V], index: Int, v: V1): Unit = node.asInstanceOf[Node[K, V1]].asLeaf.values(index) = v

  protected abstract class Node[K, V] extends Serializable {
    var parent: InternalNode[K, V]

    def isLeaf: Boolean

    var prev: Node[K, V] = _
    var next: Node[K, V] = _
    val keys = new ArrayBuffer[K]

    def length: Int = keys.size

    def asInternal: InternalNode[K, V] = asInstanceOf[InternalNode[K, V]]

    def asLeaf: LeafNode[K, V] = asInstanceOf[LeafNode[K, V]]
  }

  protected class InternalNode[K, V](var parent: InternalNode[K, V]) extends Node[K, V] {
    val isLeaf = false
    val branches = new ArrayBuffer[Node[K, V]]

    override def toString: String = keys.mkString("internal[", ", ", "]")
  }

  protected class LeafNode[K, V](var parent: InternalNode[K, V]) extends Node[K, V] {
    val isLeaf = true
    val values = new ArrayBuffer[V]

    override def toString: String = keys.mkString("leaf[", ", ", "]")
  }
}