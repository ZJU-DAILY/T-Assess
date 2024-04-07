package kit

import java.io.PrintWriter
import scala.sys.process._
import scala.util.matching.Regex.Match
import scala.annotation.tailrec
import scala.collection.AbstractIterable
import scala.collection.AbstractIterator
import scala.collection.mutable
import scala.collection.immutable.ListMap

/**
 * Provides for interaction (searching, insertion, update, deletion) with a B+ tree that can be stored anywhere (in memory, on disk).  It is the implementation's responsability to create the empty B+ tree initially. An empty B+ tree consists of a single empty leaf node as the root. Also the `first` and `last` should refer to the root leaf node, and the `lastlen` variable should be 0. For on-disk implementations it should be possible to open an existing B+ tree or optionally create a new one.
 *
 * A B+ tree is composed of nodes that are linked to one another using pointers. The exact way in which the nodes are layed out and stored is left to the implementation. Also, node pointers are left to the implementation through the abstract node type. There is a very important implementation requirement that comparing two nodes for equality return `true` when they point to the same node. All nodes have
 *    - a parent pointer
 *    - a next pointer to the right sibling
 *    - a prev pointer to the left sibling
 *    - an array of keys
 *
 * There are two kinds nodes:
 *    - leaf nodes, containing an array of values each corresponding to it's associated key in the keys array.
 *    - internal nodes, containing an array of branch pointers. This array always has one element more than the number of keys.
 *
 * @constructor creates an object providing access to a B+ tree with a branching factor of `order` and possibly creating an empty tree.
 * @tparam K the type of the keys contained in this map.
 * @tparam V the type of the values associated with the keys.
 */
abstract class BPlusTree[K <% Ordered[K], +V] extends Serializable {
  /**
   * Abstract node type. For in-memory implementations this would probably be the actual node class and for on-disk it would likely be the file pointer where the node is stored.
   *
   * @note Comparing two nodes for equality is required to be `true` when they point to the same node.
   */
  protected type N

  /**
   * Order or branching factor of the tree. The order is the maximum number of branches that an internal node can have. The maximum number of elements in a leaf node is `order - 1`.
   */
  val order: Int

  /**
   * Root node. Implementations are required to set this as well as to create/update the in-storage copy of this if needed (only really applies to on-disk implementations). The methods in this class will take care of updating this variable, implementations only need to worry about the in-storage copy.
   */
  protected var root: N

  /**
   * First leaf node. Implementations are required to set this as well as to create/update the in-storage copy of this if needed (only really applies to on-disk implementations) within the implementation's `newRoot` method. The methods in this class will take care of updating this variable, implementations only need to worry about the in-storage copy.
   */
  protected var first: N

  /**
   * Last leaf node. Implementations are required to set this as well as to create/update the in-storage copy of this if needed (only really applies to on-disk implementations). The methods in this class will take care of updating this variable, implementations only need to worry about the in-storage copy.
   */
  protected var last: N

  /**
   * Length of the last leaf node.  This just speeds up bulk loading (the `load` method). Implementations are required to set this.
   */
  protected var lastlen: Int

  /**
   * The minimum length (number of keys) that a non-root node (internal or leaf) may have is `ceil(order/2) - 1`. The minimum length for a root leaf node is 0. The minimum length for a root internal node is 1.
   */
  protected val minlen: Int = order / 2 + order % 2 - 1

  /**
   * Adds a new `branch` to (internal) `node`. `branch` is placed at an index equal to the length of `node`, given that the length of a node is the number of keys.
   */
  protected def addBranch(node: N, branch: N): Unit

  /**
   * Adds a new `key` to `node`. The length of `node` will increase by one as a result. This method should be called before either `addBranch` or `addValue` since those methods use the current node length to determine placemenet.
   */
  protected def addKey(node: N, key: K): Unit

  /**
   * Free the storage previously allocated for the key at `index` in `node`. For in-memory implementations, this method probably won't do anything.
   */
  protected def freeKey(node: N, index: Int): Unit

  /**
   * Free the storage previously allocated for `node`. For in-memory implementations, this method probably won't do anything.
   */
  protected def freeNode(node: N): Unit

  /**
   * Free the storage previously allocated for the value at `index` in `node`. For in-memory implementations, this method probably won't do anything.
   */
  protected def freeValue(node: N, index: Int): Unit

  /**
   * Returns a branch pointer from an internal node at a given `index`.  There is always one more branch pointer than there are keys in an internal node so the highest index is equal to `nodeLength( node )`.
   */
  protected def getBranch(node: N, index: Int): N

  /**
   * Returns the branches of `node` as a non-strict immutable sequence.
   */
  protected def getBranches(node: N): Seq[N]

  /**
   * Returns a key from a leaf `node` at a given `index`.
   */
  protected def getKey(node: N, index: Int): K

  /**
   * Returns the keys of `node` as a non-strict immutable sequence.
   */
  protected def getKeys(node: N): Seq[K]

  /**
   * Returns the next pointer of (leaf) `node`.
   */
  protected def getNext(node: N): N

  /**
   * Returns the parent pointer of `node`.
   */
  protected def getParent(node: N): N

  /**
   * Returns the previous leaf node link pointer of (leaf) `node`.
   */
  protected def getPrev(node: N): N

  /**
   * Returns a value from a leaf `node` at a given `index`.
   */
  protected def getValue(node: N, index: Int): V

  /**
   * Returns the values of `node` as a non-strict immutable sequence.
   */
  protected def getValues(node: N): Seq[V]

  /**
   * Inserts `key` and `branch` into (internal) `node` at `keyIndex` and `branchIndex`, respectively.
   */
  protected def insertInternal(node: N, keyIndex: Int, key: K, branchIndex: Int, branch: N): Unit

  /**
   * Inserts `key` and `value` into (leaf) `node` at `index`.
   */
  protected def insertLeaf[V1 >: V](node: N, index: Int, key: K, value: V1): Unit

  /**
   * Returns `true` if `node` is a leaf node
   */
  protected def isLeaf(node: N): Boolean

  /**
   * Moves key/branch pairs from node `src` beginning at index `begin` up to but not including index `end` to node `dst` at `index`.
   */
  protected def moveInternal(src: N, begin: Int, end: Int, dst: N, index: Int): Unit

  /**
   * Moves key/value pairs from node `src` to node `dst` beginning at index `begin` and ending up to but not including index `end`.
   */
  protected def moveLeaf(src: N, begin: Int, end: Int, dst: N, index: Int): Unit

  /**
   * Creates a new internal node with `parent` as its parent pointer.
   */
  protected def newInternal(parent: N): N

  /**
   * Creates a new leaf node with `parent` as its parent pointer.
   */
  protected def newLeaf(parent: N): N

  /**
   * Creates a new root (internal) node with `branch` as its leftmost branch pointer and `null` parent pointer. Implementations are require to update the in-storage copy of the root pointer if needed (only really applies to on-disk implementations).
   */
  protected def newRoot(branch: N): N

  /**
   * Returns the length (number of keys) of `node`. For internal nodes, the number of branch pointers will one more than the length.
   */
  protected def nodeLength(node: N): Int

  /**
   * Returns the ''null'' node pointer. For in-memory implementations this will usually be a Scala `null` value. For on-disk it would make sense for this to be `0L`.
   */
  protected def nul: N

  /**
   * Removes the key and branch pair from internal `node` at `keyIndex` and `branchIndex`, respectively. This method is perhaps poorly named: it does not remove an internal node from the tree.
   *
   * @return length of `node` after removal
   */
  protected def removeInternal(node: N, keyIndex: Int, branchIndex: Int): Int

  /**
   * Removes the key/value pair from leaf `node` at `index`. This method is perhaps poorly named: it does not remove a leaf node from the tree.
   *
   * @return length of `node` after removal
   */
  protected def removeLeaf(node: N, index: Int): Int

  /**
   * Sets the in-storage copy of the first leaf node pointer. This method is not responsable for setting the `first` variable.
   */
  protected def setFirst(leaf: N): Unit

  /**
   * Sets the key at `index` of `node` to `key`.
   */
  protected def setKey(node: N, index: Int, key: K): Unit

  /**
   * Sets the in-storage copy of the last leaf node pointer. This method is not responsable for setting the `last` variable nor the `lastlen` variable.
   */
  protected def setLast(leaf: N): Unit

  /**
   * Sets the next pointer of (leaf) `node` to `p`.
   */
  protected def setNext(node: N, p: N): Unit

  /**
   * Sets the parent pointer of `node` to `p`.
   */
  protected def setParent(node: N, p: N): Unit

  /**
   * Sets previous leaf node link pointer of (leaf) `node` to `p`.
   */
  protected def setPrev(node: N, p: N): Unit

  /**
   * Sets the in-storage copy of the root node pointer. This method is not responsable for setting the `root` variable.
   */
  protected def setRoot(node: N): Unit

  /**
   * Sets the value at `index` of `node` to `v`.
   */
  protected def setValue[V1 >: V](node: N, index: Int, v: V1): Unit


  /**
   * Returns the leaf position of the key that is least greater than or equal to `key`.
   **/
  protected def leastGTE(key: K): (N, Int) =
    lookupGTE(key) match {
      case (_, leaf, index) => (leaf, index)
    }

  /**
   * Returns the leaf position of the key that is least greater than `key`.
   **/
  protected def leastGT(key: K): (N, Int) =
    lookupGTE(key) match {
      case (true, leaf, index) => nextPosition(leaf, index)
      case (false, leaf, index) => (leaf, index)
    }

  /**
   * Returns the leaf position of the key that is greatest less than or equal to `key`.
   **/
  protected def greatestLTE(key: K): (N, Int) =
    lookupLTE(key) match {
      case (_, leaf, index) => (leaf, index)
    }

  /**
   * Returns the leaf position of the key that is greatest less than `key`.
   **/
  protected def greatestLT(key: K): (N, Int) =
    lookupLTE(key) match {
      case (true, leaf, index) => prevPosition(leaf, index)
      case (false, leaf, index) => (leaf, index)
    }

  /**
   * Returns the key/value pair whose key is least greater than or equal to `key`.
   */
  def leastGreaterThanOrEqual(key: K): Option[(K, V)] = optionalKeyValue(leastGTE(key))

  /**
   * Returns the key/value pair whose key is least greater than `key`.
   */
  def leastGreaterThan(key: K): Option[(K, V)] = optionalKeyValue(leastGT(key))

  /**
   * Returns the key/value pair whose key is greatest less than or equal to `key`.
   */
  def greatestLessThanOrEqual(key: K): Option[(K, V)] = optionalKeyValue(greatestLTE(key))

  /**
   * Returns the key/value pair whose key is greatest less than `key`.
   */
  def greatestLessThan(key: K): Option[(K, V)] = optionalKeyValue(greatestLT(key))

  /**
   * Returns a bounded iterator over a range of key/value pairs in the tree in ascending sorted key order. The range of key/value pairs in the iterator is specified by `bounds`.  `bounds` must contain one or two pairs where the first element in the pair is a symbol corresponding to the type of bound (i.e. '<, '<=, '>, '>=) and the second element is a key value.
   *
   * An example of a bounded iterator over all elements in a tree (with `String` keys) that will include all keys that sort greater than or equal to "a" and up to but not including "e" is `boundedIterator( ('>=, "a"), ('<, "e") )`.
   */
  def boundedIterator(bounds: (Symbol, K)*): Iterator[(K, V)] = boundedPositionIterator(bounds: _*) map {case (n, i) => getKeyValue(n, i)}

  /**
   * Returns a bounded iterator over a range of key positions (node/index pairs) in the tree in ascending sorted key order. The `bounds` parameter is the same as for [[boundedIterator]].
   */
  protected def boundedPositionIterator(bounds: (Symbol, K)*): Iterator[(N, Int)] = {
    val ((loleaf, loindex), (hileaf, hiindex)) = boundsPreprocess(bounds, leastGT, leastGTE)

    new AbstractIterator[(N, Int)] {
      var leaf: N = loleaf
      var index: Int = loindex

      def hasNext: Boolean = leaf != nul && index < nodeLength(leaf) && (leaf != hileaf || index < hiindex)

      def next: (N, Int) =
        if (hasNext) {
          val (n, i) = nextPosition(leaf, index)
          val cur = (leaf, index)
          leaf = n
          index = i
          cur
        } else {
          throw new NoSuchElementException( "no more keys" )
        }
    }
  }

  /**
   * Returns a bounded iterator over a range of key/value pairs in the tree in descending sorted key order. The range of key/value pairs in the iterator is specified by `bounds`.  `bounds` must contain one or two pairs where the first element in the pair is a symbol corresponding to the type of bound (i.e. '<, '<=, '>, '>=) and the second element is a key value.
   *
   * An example of a reverse bounded iterator over all elements in a tree (with `String` keys) that will include all keys that sort greater than or equal to "a" and up to but not including "e", iterated over in reverse order, is `reverseBoundedIterator( ('>=, "a"), ('<, "e") )`.
   */
  def reverseBoundedIterator(bounds: (Symbol, K)*): Iterator[(K, V)] = reverseBoundedPositionIterator(bounds: _*) map {case (n, i) => getKeyValue(n, i)}

  /**
   * Returns a bounded iterator over a range of key positions (node/index pairs) in the tree in descending sorted key order. The `bounds` parameter is the same as for [[boundedIterator]].
   */
  protected def reverseBoundedPositionIterator(bounds: (Symbol, K)*): Iterator[(N, Int)] = {
    val ((loleaf, loindex), (hileaf, hiindex)) = boundsPreprocess(bounds, greatestLTE, greatestLT)

    new AbstractIterator[(N, Int)] {
      var leaf: N = hileaf
      var index: Int = hiindex

      def hasNext: Boolean = leaf != nul && index < nodeLength(leaf) && (leaf != loleaf || index < loindex)

      def next: (N, Int) =
        if (hasNext) {
          val (n, i) = prevPosition(leaf, index)
          val cur = (leaf, index)
          leaf = n
          index = i
          cur
        } else {
          throw new NoSuchElementException( "no more keys" )
        }
    }
  }

  private def boundsPreprocess(bounds: Seq[(Symbol, K)], strictinf: K => (N, Int), inf: K => (N, Int)) = {
    require(bounds.length == 1 || bounds.length == 2, "boundedIterator: one or two bounds")

    val symbols = ListMap[Symbol, K => (N, Int)](Symbol(">") -> strictinf, Symbol(">=") -> inf, Symbol("<") -> inf, Symbol("<=") -> strictinf)

    require(bounds forall {case (s, _) => symbols contains s}, "boundedIterator: expected one of '<, '<=, '>, '>=")

    def translate(bound: Int) = symbols(bounds(bound)._1)(bounds(bound)._2)

    def order(s: Symbol) = symbols.keys.toList indexOf s

    if (bounds.length == 2) {
      require(bounds.head._1 != bounds(1)._1, "boundedIterator: expected bounds symbols to be different")

      val (lo, hi, (slo, klo), (shi, khi)) =
        if (order(bounds.head._1) > order(bounds(1)._1))
          (translate(1), translate(0), bounds(1), bounds.head)
        else
          (translate(0), translate(1), bounds.head, bounds(1))

      if (klo > khi || klo == khi && ((slo != Symbol(">=")) || (shi != Symbol("<=")))) (lo, lo)
      else (lo, hi)
    }
    else if (order( bounds.head._1 ) < 2) (translate( 0 ), (nul, 0))
    else ((first, 0), translate( 0 ))
  }

  /**
   * Returns a non-strict `Iterable` containing the keys in the tree.
   */
  def keys: Iterable[K] =
    new AbstractIterable[K] {
      def iterator: Iterator[K] = keysIterator
    }

  /**
   * Returns a bounded iterator over a range of keys in the tree in ascending sorted key order. The `bounds` parameter is the same as for [[boundedIterator]].
   */
  def boundedKeysIterator(bounds: (Symbol, K)*): Iterator[K] = boundedPositionIterator(bounds: _*) map {case (n, i) => getKey(n, i)}

  /**
   * Returns a bounded iterator over a range of values in the tree in ascending sorted key order. The `bounds` parameter is the same as for [[boundedIterator]].
   */
  def boundedValuesIterator(bounds: (Symbol, K)*): Iterator[V] = boundedPositionIterator(bounds: _*) map {case (n, i) => getValue(n, i)}

  /**
   * Returns a bounded iterator over a range of keys in the tree in descending sorted key order. The `bounds` parameter is the same as for [[boundedIterator]].
   */
  def reverseBoundedKeysIterator(bounds: (Symbol, K)*): Iterator[K] = reverseBoundedPositionIterator(bounds: _*) map {case (n, i) => getKey(n, i)}

  /**
   * Returns a bounded iterator over a range of values in the tree in descending sorted key order. The `bounds` parameter is the same as for [[boundedIterator]].
   */
  def reverseBoundedValuesIterator(bounds: (Symbol, K)*): Iterator[V] = reverseBoundedPositionIterator(bounds: _*) map {case (n, i) => getValue(n, i)}

  /**
   * Returns `true` is the tree is empty.
   */
  def isEmpty: Boolean = lastlen == 0

  /**
   * Returns an iterator over all key/value pairs in the tree in ascending sorted key order.
   */
  def iterator: Iterator[(K, V)] = positionIterator map {case (n, i) => getKeyValue(n, i)}

  /**
   * Returns an iterator over all key/value pairs in the tree in descending sorted key order.
   */
  def reverseIterator: Iterator[(K, V)] = reversePositionIterator map {case (n, i) => getKeyValue(n, i)}

  /**
   * Returns an iterator over all key positions (node/index pairs) in the tree in ascending sorted key order.
   */
  protected def positionIterator: Iterator[(N, Int)] =
    new AbstractIterator[(N, Int)] {
      var leaf: N = first
      var index: Int = 0

      def hasNext: Boolean = leaf != nul && index < nodeLength(leaf)

      def next: (N, Int) =
        if (hasNext) {
          val (n, i) = nextPosition(leaf, index)
          val cur = (leaf, index)
          leaf = n
          index = i
          cur
        } else {
          throw new NoSuchElementException( "no more keys" )
        }
    }

  /**
   * Returns a reverse iterator over all key positions (node/index pairs) in the tree in descending sorted key order.
   */
  protected def reversePositionIterator: Iterator[(N, Int)] =
    new AbstractIterator[(N, Int)] {
      var leaf: N = last
      var index: Int = lastlen - 1

      def hasNext: Boolean = leaf != nul && index >= 0

      def next: (N, Int) =
        if (hasNext) {
          val (n, i) = prevPosition(leaf, index)
          val cur = (leaf, index)
          leaf = n
          index = i
          cur
        } else {
          throw new NoSuchElementException( "no more keys" )
        }
    }

  /**
   * Returns an iterator over all keys in the tree in ascending sorted order.
   */
  def keysIterator: Iterator[K] = positionIterator map {case (n, i) => getKey(n, i)}

  /**
   * Returns an iterator over all values in the tree in the order corresponding to ascending keys.
   */
  def valuesIterator: Iterator[V] = positionIterator map {case (n, i) => getValue(n, i)}

  /**
   * Returns a reverse iterator over all keys in the tree in descending sorted order.
   */
  def reverseKeysIterator: Iterator[K] = reversePositionIterator map {case (n, i) => getKey(n, i)}

  protected def optionalKeyValue(pos: (N, Int)): Option[(K, V)] =
    pos match {
      case (null, _) => None
      case (leaf, index) =>
        if (index < nodeLength(leaf))
          Some(getKeyValue(leaf, index))
        else
          None
    }

  protected def optionalKey(pos: (N, Int)): Option[K] =
    pos match {
      case (null, _) => None
      case (leaf, index) =>
        if (index < nodeLength(leaf))
          Some(getKey(leaf, index))
        else
          None
    }

  /**
   * Returns the maximum key and it's associated value.
   *
   * @return `Some( (key, value) )` where `key` is the maximum key and `value` is it's associated value if the tree is non-empty, or `None` if the tree is empty.
   */
  def max: Option[(K, V)] =
    if (isEmpty)
      None
    else
      Some(getKeyValue(last, lastlen - 1))

  /**
   * Returns the maximum key.
   */
  def maxKey: Option[K] =
    if (isEmpty)
      None
    else
      Some(getKey(last, lastlen - 1))

  /**
   * Returns the minimum key and it's associated value.
   *
   * @return `Some( (key, value) )` where `key` is the minimum key and `value` is it's associated value if the tree is non-empty, or `None` if the tree is empty.
   */
  def min: Option[(K, V)] = optionalKeyValue(first, 0)

  /**
   * Returns the minimum key.
   */
  def minKey: Option[K] = optionalKey(first, 0)

  /**
   * Inserts `key` with associated `value` into the tree. If `key` exists, then it's new associated value will be `value`.
   *
   * @return `true` if `key` exists
   */
  def insert[V1 >: V]( key: K, value: V1 = null.asInstanceOf[V1] ): Boolean =
    lookup( key ) match {
      case (true, leaf, index) =>
        setValue(leaf, index, value)
        true
      case (false, leaf, index) =>
        insertAt(key, value, leaf, index)
        false
    }

  /**
   * Inserts `key` with associated `value` into the tree only if `key` does not exist.
   *
   * @return `true` if `key` exists
   */
  def insertIfNotFound[V1 >: V]( key: K, value: V1 = null.asInstanceOf[V1] ): Boolean =
    lookup(key) match {
      case (true, _, _) => true
      case (false, leaf, index) =>
        insertAt(key, value, leaf, index)
        false
    }

  /**
   * Inserts `keys` into the tree each with an associated value of `null`. If a given key exists, then it's new associated value will be `null`.
   */
  def insertKeys(keys: K*): Unit =
    for (k <- keys)
      insert(k, null.asInstanceOf[V])

  /**
   * Inserts `keys` into the tree each with an associated value of `null`, and checks that the tree is well constructed after each key is inserted. If a given key exists, then it's new associated value will be `null`. This method is used for testing.
   */
  def insertKeysAndCheck(keys: K*): String = {
    for (k <- keys) {
      insert(k, null.asInstanceOf[V])
      wellConstructed match {
        case "true" =>
        case reason => return reason + " after inserting key " + k
      }
    }
    "true"
  }

  /**
   * Performs the B+ tree bulk loading algorithm to insert key/value pairs `kvs` into the tree efficiently. This method is more efficient than using `insert` because `insert` performs a search to determine the correct insertion point for the key whereas `load` does not. `load` can only work if the tree is empty, or if the minimum key to be inserted is greater than the maximum key in the tree.
   */
  def load[V1 >: V](kvs: (K, V1)* ) {
    require(kvs.nonEmpty, "expected some key/value pairs to load")

    val seq = kvs sortBy {case (k, _) => k}

    maxKey match {
      case None =>
      case Some(maxkey) => require(maxkey < seq.head._1, "can only load into non-empty tree if maximum element is less than minimum element to be loaded")
    }

    seq foreach {case (k, v) => insertAt(k, v, last, lastlen)}
  }

  /**
   * Searches for `key` returning it's associated value if `key` exists.
   *
   * @return `Some( value )` where `value` is the value associated to `key` if it exists, or `None` otherwise
   */
  def search(key: K): Option[V] =
    lookup(key) match {
      case (true, leaf, index) => Some(getValue(leaf, index))
      case _ => None
    }

  /**
   * Performs a binary search for key `target` within `node` (tail recursively).
   *
   * @return the index of `target` within `node` if it exists, or (-''insertionPoint'' - 1) where ''insertionPoint'' is the index of the correct insertion point for key `target`.
   */
  protected def binarySearch(node: N, target: K): Int = {
    @tailrec
    def search(start: Int, end: Int ): Int = {
      if (start > end)
        -start - 1
      else {
        val mid = start + (end - start + 1) / 2

        if (getKey(node, mid) == target)
          mid
        else if (getKey(node, mid) > target)
          search(start, mid - 1)
        else
          search(mid + 1, end)
      }
    }

    search(0, nodeLength(node) - 1)
  }

  /**
   * Performs the B+ tree lookup algorithm (tail recursively) beginning at the root, in search of the location (if found) or correct insertion point (if not found) of `key`.
   *
   * @return a triple where the first element is `true` if `key` exists and `false` otherwise, the second element is the node containing `key` if found or the correct insertion point for `key` if not found, the third is the index within that node.
   */
  protected def lookup(key: K): (Boolean, N, Int) = {
    @tailrec
    def _lookup(n: N ): (Boolean, N, Int) =
      if (isLeaf( n ))
        binarySearch(n, key) match {
          case index if index >= 0 => (true, n, index)
          case index => (false, n, -(index + 1))
        }
      else
        binarySearch(n, key) match {
          case index if index >= 0 => _lookup(getBranch(n, index + 1))
          case index => _lookup(getBranch(n, -(index + 1)))
        }

    _lookup(root)
  }

  /**
   * Returns the key/value pair at `index` within `leaf`.
   */
  protected def getKeyValue(leaf: N, index: Int): (K, V) = (getKey(leaf, index), getValue(leaf, index))

  /**
   * Returns the node/index pair pointing to the location of the leaf node key preceding the one at `index` in `leaf`.
   */
  protected def prevPosition(leaf: N, index: Int): (N, Int) =
    if (index == 0) {
      val prev = getPrev( leaf )

      (prev, if (prev == nul) 0 else nodeLength(prev) - 1)
    } else {
      (leaf, index - 1)
    }

  /**
   * Returns the node/index pair pointing to the location of the leaf node key following the one at `index` in `leaf`.
   */
  protected def nextPosition(leaf: N, index: Int): (N, Int) =
    if (index == nodeLength(leaf) - 1)
      (getNext(leaf), 0)
    else
      (leaf, index + 1)

  /**
   * Searches for `key` returning a point in a leaf node that is the greatest less than (if not found) or equal to (if found) `key`. The leaf node and index returned in case `key` does not exist is not necessarily the correct insertion point. This method is used by `reverseBoundedIterator`.
   *
   * @return a triple where the first element is `true` if `key` exists and `false` otherwise, and the second element is the leaf node containing the greatest less than or equal key, and the third is the index of that key.
   */
  protected def lookupLTE(key: K): (Boolean, N, Int) =
    lookup(key) match {
      case t@(true, _, _) => t
      case f@(false, leaf, index) =>
        val (l, i) = prevPosition(leaf, index)

        (false, l, i)
    }

  /**
   * Searches for `key` returning a point in a leaf node that is the least greater than (if not found) or equal to (if found) `key`. The leaf node and index returned in case `key` does not exist is not necessarily the correct insertion point. This method is used by `boundedIterator`.
   *
   * @return a triple where the first element is `true` if `key` exists and `false` otherwise, and the second element is the leaf node containing the least greater than or equal key, and the third is the index of that key.
   */
  protected def lookupGTE(key: K): (Boolean, N, Int) =
    lookup(key) match {
      case t@(true, _, _) => t
      case f@(false, leaf, index) =>
        if (index < nodeLength(leaf))
          f
        else
          (false, getNext(leaf), 0)
    }

  /**
   * Performs the B+ tree insertion algorithm to insert `key` and associated `value` into the tree, specifically in `leaf` at `index`, rebalancing the tree if necessary. If `leaf` and `index` is not the correct insertion point for `key` then this method will probably result in an invalid B+ tree.
   */
  protected def insertAt[V1 >: V](key: K, value: V1, leaf: N, index: Int): Unit = {
    def split = {
      val newleaf = newLeaf(getParent(leaf))
      val leafnext = getNext(leaf)

      setNext(newleaf, leafnext)

      if (leafnext == nul) {
        last = newleaf
        setLast(newleaf)
      } else
        setPrev(leafnext, newleaf)

      setNext(leaf, newleaf)
      setPrev(newleaf, leaf)

      val len = nodeLength(leaf)
      val mid = len / 2

      if (leafnext == nul)
        lastlen = len - mid

      moveLeaf(leaf, mid, len, newleaf, 0)
      newleaf
    }

    insertLeaf(leaf, index, key, value)

    if (leaf == last)
      lastlen += 1

    if (nodeLength(leaf) == order) {
      if (getParent(leaf) == nul) {
        root = newRoot(leaf)
        setParent(leaf, root)

        val newleaf = split

        insertInternal(root, 0, getKey(newleaf, 0), 1, newleaf)
      } else {
        var par = getParent( leaf )
        val newleaf = split

        binarySearch(par, getKey(newleaf, 0)) match {
          case index if index >= 0 => sys.error( "key found in internal node" )
          case insertion =>
            insertInternal(par, -(insertion + 1), getKey(newleaf, 0), -(insertion + 1) + 1, newleaf)

            while (nodeLength(par) == order) {
              val parpar = getParent(par)
              val newinternal = newInternal(parpar)
              val len = nodeLength(par)
              val mid = len/2
              val middle = getKey(par, mid)

              addBranch(newinternal, getBranch(par, mid + 1))
              freeKey(par, mid)
              removeInternal(par, mid, mid + 1)
              moveInternal(par, mid, len - 1, newinternal, 0)

              for (child <- getBranches(newinternal))
                setParent(child, newinternal)

              if (!isLeaf(getBranch(par, 0))) {
                setNext(getBranch(par, nodeLength(par)), nul)	// compute nodeLength( par )
                setPrev(getBranch(newinternal, 0), nul)
              }

              val internalnext = getNext(par)

              setNext(newinternal, internalnext)

              if (internalnext != nul)
                setPrev(internalnext, newinternal)

              setNext(par, newinternal)
              setPrev(newinternal, par)

              if (getParent(par) == nul) {
                root = newRoot(par)

                setParent(newinternal, root)
                setParent(par, root)
                insertInternal(root, 0, middle, 1, newinternal)
                par = root
              } else {
                par = parpar
                binarySearch(par, middle) match {
                  case index if index >= 0 => sys.error( "key found in internal node" )
                  case insertion => insertInternal(par, -(insertion + 1), middle, -(insertion + 1) + 1, newinternal)
                }
              }
            }
        }
      }
    }
  }

  /**
   * Returns the left most or least key within or under `node`.
   */
  protected def leftmost(node: N): K =
    if (isLeaf(node))
      getKey(node, 0)
    else
      leftmost(getBranch(node, 0))

  /**
   * Returns the right most or greatest key within or under `node`.
   */
  protected def rightmost(node: N): K =
    if (isLeaf(node))
      getKey(node, nodeLength(node) - 1)
    else
      rightmost(getBranch(node, nodeLength(node)))


  /**
   * Performs the B+ tree deletion algorithm to remove `key` and it's associated value from the tree, rebalancing the tree if necessary.
   *
   * @return `true` if `key` was found (and therefore removed), `false` otherwise
   */
  def delete(key: K): Boolean = {
    lookup(key) match {
      case (true, leaf, index) =>
        freeKey(leaf, index)
        freeValue(leaf, index)

        val len = removeLeaf(leaf, index)

        if (leaf != root && len < minlen) {
          var par = getParent(leaf)
          val (sibling, leafside, siblingside, left, right, parkey) = {
            val next = getNext(leaf)

            if (next != nul && getParent(next) == par) {
              (next, len, 0, leaf, next, if (len == 0) key else getKey(leaf, 0))
            } else {
              val prev = getPrev(leaf)

              if (prev != nul && getParent(prev) == par)
                (prev, 0, nodeLength(prev) - 1, prev, leaf, getKey(prev, 0))
              else
                sys.error( "no sibling" )
            }
          }

          val index =
            binarySearch(par, parkey) match {
              case ind if ind >= 0 => ind + 1 // add one because if it's found then it's the wrong one
              case ind => -(ind + 1)
            }

          if (nodeLength(sibling) > minlen) {
            //						println( "borrow from " + str(sibling) + " to " + str(leaf) )
            moveLeaf(sibling, siblingside, siblingside + 1, leaf, leafside)
            //						println( getKey(right, 0) )
            setKey(par, index, getKey(right, 0))

            if (leaf == last)
              lastlen = nodeLength(leaf)
            else if (sibling == last)
              lastlen = nodeLength(sibling)
          } else {
            //						println("merge " + left + " " + right)
            moveLeaf(right, 0, nodeLength(right), left, nodeLength(left))

            val next = getNext(right)

            setNext(left, next)

            if (next == nul) {
              last = left
              setLast(left)
              lastlen = nodeLength(left)
            } else
              setPrev(next, left)

            freeNode(right)

            //						println("remove (leaf) " + par + " " + index)
            freeKey(par, index)

            var len = removeInternal(par, index, index + 1)

            if (par == root && len == 0) {
              freeNode(root)
              setParent(left, nul)
              root = left
              setRoot(left)
              first = left
              setFirst(left)
            } else
              while (par != root && len < minlen) {
                val internal = par
                val (sibling, internalsidekey, siblingsidekey, internalsidebranch, siblingsidebranch, left, right, parkey, branch, keytoadd, keytoset) = {
                  val next = getNext(internal)

                  if (next != nul) {
                    val br = getBranch(next, 0)

                    (next, len, 0, len + 1, 0, internal, next, if (len == 0) key else getKey(internal, 0), br, leftmost(br), leftmost(getBranch(next, 1)))
                  } else {
                    val prev = getPrev(internal)
                    val prevlen = nodeLength(prev)
                    val br = getBranch(prev, prevlen)
                    //										println("* keytoadd " + internal + " " + leftmost( internal ))
                    //										println("* keytoset " + internal + " " + leftmost( br ))

                    if (prev != nul)
                      (prev, 0, prevlen - 1, 0, prevlen, prev, internal, getKey(prev, 0), br, leftmost(internal), leftmost(br))
                    else
                      sys.error( "no sibling" )
                  }
                }

                par = getParent(par)

                val index =
                  binarySearch(par, parkey) match {
                    case ind if ind >= 0 => ind + 1 // add one because if it's found then it's the wrong one
                    case ind => -(ind + 1)
                  }

                if (nodeLength(sibling) > minlen) {
                  // 						println("borrow " + internal + " " + sibling)
                  // 						println("insertKey " + internal + " " + keytoadd)
                  insertInternal(internal, internalsidekey, keytoadd, internalsidebranch, branch)
                  setParent(branch, internal)
                  freeKey(sibling, siblingsidekey)
                  removeInternal(sibling, siblingsidekey, siblingsidebranch)
                  setKey(par, index, keytoset)

                  if (!isLeaf(getBranch(sibling, 0))) {
                    for ((l, r) <- getBranches(internal) dropRight 1 zip (getBranches(internal) drop 1)) {
                      setNext(l, r)
                      setPrev(r, l)
                    }

                    for ((l, r) <- getBranches(sibling) dropRight 1 zip (getBranches(sibling) drop 1)) {
                      setNext(l, r)
                      setPrev(r, l)
                    }

                    setPrev(getBranch(sibling, 0), nul)
                    setNext(getBranch(sibling, nodeLength(sibling)), nul)
                    setPrev(getBranch(internal, 0), nul)
                    setNext(getBranch(internal, nodeLength(internal)), nul)
                  }

                  par = root
                } else {
                  // 						println("merge " + left + " " + right)
                  // 						println("addKey " + leftmost(right))
                  addKey(left, leftmost(right))

                  val middle = getBranch(right, 0)

                  addBranch(left, middle)
                  moveInternal(right, 0, nodeLength(right), left, nodeLength(left))
                  getBranches(left) drop 1 foreach (setParent(_, left))

                  freeNode(right)

                  if (!isLeaf(getBranch(left, 0)))
                    for ((l, r) <- getBranches(left) dropRight 1 zip (getBranches(left) drop 1)) {
                      setNext(l, r)
                      setPrev(r, l)
                    }

                  setNext(left, nul)
                  //						println("remove (internal) " + par + " " + index)
                  freeKey(par, index)
                  len = removeInternal(par, index, index + 1)

                  if (par == root) {
                    if (len == 0) {
                      freeNode(root)
                      setParent(left, nul)
                      root = left
                      setRoot(left)
                      par = root
                    } else if (!isLeaf(getBranch(root, 0)))
                      for ((l, r) <- getBranches(root) dropRight 1 zip (getBranches(root) drop 1)) {
                        setNext(l, r)
                        setPrev(r, l)
                      }
                  } else {
                    if (len >= minlen)
                      if (!isLeaf(getBranch(par, 0)))
                        for ((l, r) <- getBranches(par) dropRight 1 zip (getBranches(par) drop 1)) {
                          setNext(l, r)
                          setPrev(r, l)
                        }
                  }
                }
              }
          }
        } else if (leaf == last)
          lastlen -= 1

        true
      case (false, leaf, index) => false
    }
  }

  /**
   * Analyzes the tree to determine if it is well constructed.
   *
   * @return `"true"` (as a string) if the tree is a well constructed B+ tree, a string description of the flaw otherwise.
   */
  def wellConstructed: String = {
    var depth = -1
    var prevnode: N = nul
    var nextptr: N = nul

    def check(n: N, p: N, d: Int): String = {
      if (!(getKeys(n) dropRight 1 zip (getKeys(n) drop 1) forall {case (n1, n2) => n1 < n2}))
        return "incorrectly ordered keys"

      if (getParent(n) != p)
        return "incorrect parent pointer in level " + d

      if (isLeaf(n)) {
        if (depth == -1)
          depth = d
        else if (d != depth)
          return "leaf nodes not at same depth"

        if (getParent(n) == nul) {
          if (nodeLength(n) >= order)
            return "root leaf node length out of range"
        } else if (nodeLength(n) < minlen || nodeLength(n) > order - 1)
          return "non-root leaf node length out of range"

        if (prevnode == nul && first != n)
          return "incorrect first pointer"

        if (prevnode != getPrev(n))
          return "incorrect prev pointer"
        else
          prevnode = n

        if (getNext(n) == nul && last != n)
          return "incorrect last pointer"

        if ((nextptr != nul) && (nextptr != n))
          return "incorrect next pointer"
        else
          nextptr = getNext(n)

        if (getNext(n) != nul && getKeys(n).last > getKey(getNext(n), 0) || getPrev(n) != nul && getKeys(getPrev(n)).last > getKey(n, 0))
          return "leaf node last key not less than or equal to next leaf node first key"
      } else {
        if (getBranches(n) contains nul)
          return "null branch pointer"

        if (getKeys(getBranch(n, 0)).isEmpty)
          return "empty internal node branch"

        if (getKeys(n).isEmpty)
          return "empty internal node"

        if (!isLeaf(getBranch(n, 0))) {
          if (!(getBranches(n) dropRight 1 zip (getBranches(n) drop 1) forall {case (n1, n2) => getNext(n1) == n2}) ||
            getNext(getBranches(n).last) != nul)
            return "incorrect next pointer"

          if (!(getBranches( n ) dropRight 1 zip (getBranches(n) drop 1) forall {case (n1, n2) => n1 == getPrev(n2)}) ||
            getPrev(getBranch(n, 0)) != nul)
            return "incorrect prev pointer"
        }

        if (getParent(n) == nul) {
          if (getPrev(n) != nul)
            return "non-null prev pointer"

          if (getNext(n) != nul)
            return "non-null next pointer"

          if (nodeLength(n) < 1 || nodeLength(n) > order - 1)
            return "root internal node length out of range"
        } else {
          if (nodeLength(n) < minlen || nodeLength(n) > order - 1)
            return "non-root internal node length out of range: " + nodeLength(n) + ", " + n
        }

        if (!(getKeys(n) zip (getBranches(n) dropRight 1) forall {case (k, b) => k > rightmost(b) && k > getKey(b, 0)}))
          return "left internal node branch not strictly less than: " + n

        if (!(getKeys(n) zip (getBranches(n) drop 1) forall {case (k, b) => k <= leftmost(b) && k <= getKey(b, 0)}))
          return "right internal node branch not greater than or equal: " + n

        for (b <- getBranches(n))
          check(b, n, d + 1) match {
            case "true" =>
            case error => return error
          }
      }

      "true"
    }

    check(root, nul, 0) match {
      case "true" =>
      case error => return error
    }

    if (nextptr != nul)
      return "rightmost next pointer not null"

    "true"
  }

  /**
   * Performs a breadth first traversal of the tree (tail recursively), applying `level` to each level of the tree beginning at the root.
   */
  protected def traverseBreadthFirst(level: List[N] => Unit): Unit = {
    @tailrec
    def traverse(nodes: List[N] ): Unit = {
      level(nodes)

      if (!isLeaf(nodes.head))
        traverse(nodes flatMap getBranches)
    }

    traverse(List(root))
  }

  /**
   * Returns a string representing a search result for `key` that will be consistant with `prettyPrint`. This method is used mainly for unit testing.
   */
  def prettySearch(key: K): String = {
    val map = new mutable.HashMap[N, String]
    var count = 0

    traverseBreadthFirst(
      nodes =>
        for (n <- nodes) {
          map(n) = "n" + count
          count += 1
        }
    )

    lookup(key) match {
      case (true, leaf, index) => map(leaf) + " " + getValue(leaf, index) + " " + index
      case _ => "not found"
    }
  }

  /**
   * Returns a serialization (string representation of the tree) using string and function arguments to specify the exact form of the serialization. This method is used for ''pretty printing'' and to generate a DOT (graph description language) description of the tree so that it can be visualized.
   *
   * @param before string to be prepended to the serialization
   * @param prefix string to be place before each line of the serialization that includes internal and leaf nodes
   * @param internalnode function to generate internal node serializations using three parameters: the current node, a function to return a string id of a node, a function that allows a line of text to be appended after all nodes have been serialized
   * @param leafnode function to generate leaf node serializations using two parameters: the current node, a function to return a string id of a node
   * @param after string to be appended to the serialization
   */
  protected def serialize(before: String, prefix: String, internalnode: (N, N => String, String => Unit) => String, leafnode: (N, N => String) => String, after: String): String = {
    val buf = new StringBuilder(before)
    val afterbuf = new StringBuilder
    val map = new mutable.HashMap[N, String]
    var count = 0

    def id(node: N) =
      if (node == nul)
        "null"
      else
        map get node match {
          case Some(n) => n
          case None =>
            val n = "n" + count
            map(node) = n
            count += 1
            n
        }

    def emit(s: String) = {
      afterbuf ++= prefix
      afterbuf ++= s
      afterbuf += '\n'
    }

    def printNodes(nodes: List[N]): Unit = {
      if (isLeaf(nodes.head)) {
        buf ++= prefix
        buf ++= nodes map (n => leafnode(n, id)) mkString " "
      } else {
        buf ++= prefix
        buf ++= nodes map (n => internalnode(n, id, emit)) mkString " "
        buf += '\n'
      }
    }

    traverseBreadthFirst(printNodes)

    if (afterbuf.nonEmpty || after != "")
      buf += '\n'

    buf ++= afterbuf.toString
    buf ++= after
    buf.toString
  }

  /**
   * Prints (to stdout) a readable representation of the structure and contents of the tree.
   */
  def prettyPrint(): Unit = println(prettyStringWithValues())

  /**
   * Prints (to stdout) a readable representation of the structure and contents of the tree, omitting the values and only printing the keys.
   */
  def prettyPrintKeysOnly(): Unit = println(prettyString())

  /**
   * Returns a string containing a readable representation of the structure and contents of the tree, omitting the values and only printing the keys. This method is used mainly for unit testing.
   */
  private def prettyString(): Unit = serialize( "", "", (n, id, _) => "[" + id(n) + ": (" + id(getPrev(n)) + ", " + id(getParent(n)) + ", " + id(getNext(n)) + ") " + id(getBranch(n, 0)) + " " + getKeys(n).zipWithIndex.map({case (k, j) => "| " + k + " | " + id(getBranch(n, j + 1))}).mkString(" ") + "]", (n, id) => "[" + id(n) + ": (" + id(getPrev(n)) + ", " + id(getParent(n)) + ", " + id(getNext(n)) + ")" + (if (nodeLength(n) == 0) "" else " ") + getKeys(n).mkString(" ") + "]", "" )

  /**
   * Returns a string containing a readable representation of the structure and contents of the tree. This method is used mainly for unit testing.
   */
  def prettyStringWithValues(): String = serialize( "", "", (n, id, _) => "[" + id(n) + ": (" + id(getPrev(n)) + ", " + id(getParent(n)) + ", " + id(getNext(n)) + ") " + id(getBranch(n, 0)) + " " + getKeys(n).zipWithIndex.map({case (k, j) => "| " + k + " | " + id(getBranch(n, j + 1))}).mkString(" ") + "]", (n, id) => "[" + id(n) + ": (" + id(getPrev(n)) + ", " + id(getParent(n)) + ", " + id(getNext(n)) + ")" + (if (nodeLength(n) == 0) "" else " ") + (getKeys(n) zip getValues(n) map (p => "<" + p._1 + ", " + p._2 + ">") mkString " ") + "]", "" )

  /**
   * Creates a PNG image file called `name` (with `.png` added) which visually represents the structure and contents of the tree, only showing the keys. This method uses GraphViz (specifically the `dot` command) to produce the diagram, and ImageMagik (specifically the `convert` command) to convert it from SVG to PNG. `dot` can product PNG files directly but I got better results producing SVG and converting to PNG.
   */
  def diagram(name: String ) {
    val before =
      """	|digraph {
					|    graph [splines=line];
					|    edge [penwidth=2];
					|    node [shape = record, height=.1, width=.1, penwidth=2, style=filled, fillcolor=white];
					|
					|""".stripMargin

    def internalnode(n: N, id: N => String, emit: String => Unit) = {
      val buf = new StringBuilder(id(n) + """[label = "<b0> &bull;""")

      emit( id(n) + ":b0" + " -> " + id(getBranch(n, 0)) + ";" )

      for ((k, i) <- getKeys(n).zipWithIndex) {
        buf ++= " | " + k + " | <b" + (i + 1) + "> &bull;"
        emit(id(n) + ":b" + (i + 1) + " -> " + id(getBranch(n, i + 1)) + ";")
      }

      buf ++= """"];"""
      buf.toString
    }

    // 		def leafnode( n: N, id: N => String ) = id(n) + """[label = "<prev> &bull; | """ + (getKeys(n) mkString " | ") + """ | <next> &bull;"];"""
    def leafnode(n: N, id: N => String) = id(n) + """[label = """" + (getKeys(n) mkString " | ") + """"];"""

    val file = new PrintWriter( name + ".dot" )

    file.println(serialize(before, "    ", internalnode, leafnode, "}"))
    file.close()
    s"dot -Tsvg $name.dot -o $name.svg".!
    s"convert $name.svg $name.png".!
  }

  /**
   * Returns a B+ tree build from a string representation of the tree. The syntax of the input string is simple: internal nodes are coded as lists of nodes alternating with keys (alpha strings with no quotation marks) using parentheses with elements separated by space, leaf nodes are coded as lists of alpha strings (no quotation marks) using brackets with elements separated by space.
   *
   * @example
   *
   * {{{
   * (
   *   [g] j [j t] u [u v]
   * )
   * }}}
   *
   * produces a tree that pretty prints as
   *
   * {{{
   * [n0: (null, null, null) n1 | j | n2 | u | n3]
   * [n1: (null, n0, n2) g] [n2: (n1, n0, n3) j t] [n3: (n2, n0, null) u v]
   * }}}
   */
  def build(s: String): BPlusTree[K, V] = {
    val it = """[a-zA-Z0-9]+|\n|.""".r.findAllMatchIn(s) filterNot (m => m.matched.head.isWhitespace)
    var prev: N = nul

    def readKey(key: String) =
      if (key forall (_.isDigit))
        key.toInt.asInstanceOf[K]
      else
        key.asInstanceOf[K]

    def internal(it: Iterator[Match], node: N): N =
      it.next.matched match {
        case "(" =>
          addBranch(node, internal(it, newInternal(node)))
          internal(it, node)
        case "[" =>
          addBranch(node, leaf(it, newLeaf(node)))
          internal(it, node)
        case ")" =>
          if (!isLeaf(getBranch(node, 0))) {
            getBranches(node) dropRight 1 zip (getBranches(node) drop 1) foreach {
              case (n1, n2) =>
                setNext( n1, n2 )
                setPrev( n2, n1 )
            }
          }

          node
        case key if key.head.isLetterOrDigit =>
          addKey(node, readKey(key))
          internal(it, node)
        case t => sys.error("unexpected token: " + t)
      }

    @tailrec
    def leaf(it: Iterator[Match], node: N): N =
      it.next.matched match {
        case "]" =>
          last = node
          lastlen = nodeLength(node)

          if (first == nul)
            first = node

          if (prev != nul)
            setNext(prev, node)

          setPrev(node, prev)
          prev = node
          node
        case key if key.head.isLetterOrDigit =>
          insertLeaf(node, nodeLength(node), readKey(key), null.asInstanceOf[V])
          leaf(it, node)
        case t => sys.error("unexpected token: '" + t + "'")
      }

    freeNode(root)
    first = nul
    root =
      it.next.matched match {
        case "(" => internal(it, newInternal(nul))
        case "[" => leaf(it, newLeaf(nul))
        case t => sys.error("unexpected token: " + t.head.toInt)
      }

    wellConstructed match {
      case "true" => this
      case reason => sys.error( reason )
    }
  }
}