package gr.uoa.di.mde515.trees;

import gr.uoa.di.mde515.engine.Engine;
import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.engine.buffer.BufferManager;
import gr.uoa.di.mde515.engine.buffer.IntegerSerializer;
import gr.uoa.di.mde515.engine.buffer.Page;
import gr.uoa.di.mde515.engine.buffer.RecordsPage;
import gr.uoa.di.mde515.engine.buffer.Serializer;
import gr.uoa.di.mde515.files.IndexDiskFile;
import gr.uoa.di.mde515.index.Record;
import gr.uoa.di.mde515.locks.DBLock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Disk resident B plus tree. When a node that accepts an EVEN number of items
 * splits the middle node is moved to the RIGHT sibling. For instance (for max
 * number of items equal to 4 (N=2, 2*N + 1 = 5)):
 *
 * <pre>
 * 12345 -> 12 345
 * </pre>
 *
 * When 2 nodes merge (in case the siblings of a node (from the _same_ parent -
 * one of them may be null) have not enough nodes to borrow one so the node
 * merges with one of the siblings) it is always the right node that is deleted
 *
 * <pre>
 * (nodes ijk)
 * i12 j34 k56  -> i12 j356
 * i34 j56 -> i346
 * </pre>
 *
 * @param <K>
 *            the type of the key of the one and only file
 * @param <T>
 *            the type of the value of the records to be stored in the leaf
 *            nodes - when the tree is used as an Index this corresponds to the
 *            id of the page (of the data file) the key is located
 */
public final class BPlusDisk<K extends Comparable<K>, T> {

	private static final BufferManager buf = BufferManager.getInstance();
	private final IndexDiskFile file;
	// fields - TODO private lock + thread safety
	private volatile Node<?> root;
	private AtomicInteger nodeId = new AtomicInteger(0);
	// Used to write K and T to disc and read them back
	private final Serializer<K> serKey;
	private final Serializer<T> serVal;

	/**
	 * The BPlus writes the last nodeId and root node id to files and reads them
	 * from there on restart of the engine. This class is responsible for these
	 * operations.
	 */
	private static final class Root {

		private static final Path ROOT_PATH = FileSystems.getDefault().getPath(
			"root.txt");
		private static final Path NODES_PATH = FileSystems.getDefault()
			.getPath("nodes.txt");

		static int rootFromFile() throws IOException {
			byte[] readAllBytes = Files.readAllBytes(ROOT_PATH);
			return ByteBuffer.wrap(readAllBytes).getInt();
		}

		static void rootToFile(int newRoot) throws IOException {
			Files.write(ROOT_PATH, ByteBuffer.allocate(4).putInt(newRoot)
				.array());
		}

		static int nodesFromFile() throws IOException {
			byte[] readAllBytes = Files.readAllBytes(NODES_PATH);
			return ByteBuffer.wrap(readAllBytes).getInt();
		}

		static void nodesToFile(int newRoot) throws IOException {
			Files.write(NODES_PATH, ByteBuffer.allocate(4).putInt(newRoot)
				.array());
		}
	}

	public BPlusDisk(IndexDiskFile file, Serializer<K> serKey,
			Serializer<T> serVal) throws IOException, InterruptedException {
		this.serKey = serKey;
		this.serVal = serVal;
		this.file = file;
		if (file.read() != -1) {
			System.out.println(file + " already exists");
			nodeId.set(Root.nodesFromFile());
			int rootFromFile = Root.rootFromFile();
			Page allocFrame = buf.allocFrame(rootFromFile, file);
			boolean leaf = allocFrame.readByte(Node.LEAF_OFFSET) == 1;
			setRoot((leaf) ? new LeafNode(rootFromFile) : new InternalNode(
				rootFromFile));
			// FIXME permanent alloc (and something else I forgot ...)
		} else { // FILE EMPTY - CREATE THE ROOT
			System.out.println(file + ": Creating...");
			setRoot(new LeafNode(null)); // null transaction !
			buf.flushPage(-1, file); // TODO wild flush
		}
	}

	// =========================================================================
	// API
	// =========================================================================
	public void flush(List<Integer> pageIds) throws IOException {
		for (int pageID : pageIds) {
			buf.flushPage(pageID, file);
			System.out.println("PID " + pageID);
			buf.unpinPage(pageID);
		}
		Root.rootToFile(root.getPageId());
		Root.nodesToFile(nodeId.get());
	}

	public <R extends Record<K, T>> void insert(Transaction tr, R rec)
			throws IOException, InterruptedException {
		setRoot(root.newNodeFromDiskOrBuffer(tr, DBLock.E, root.getPageId()));
		final LeafNode leafNode = root.findLeaf(tr, DBLock.E, rec.getKey());
		_insertInLeaf(tr, rec, leafNode);
	}

	public void delete(Transaction tr, K key) throws IOException,
			InterruptedException {
		root = root.newNodeFromDiskOrBuffer(tr, DBLock.E, root.getPageId());
		final LeafNode leafNode = root.findLeaf(tr, DBLock.E, key);
		// FIXME lock the siblings too!!
		_deleteInLeaf(tr, key, leafNode);
	}

	public void abort(List<Integer> pageIds) throws IOException {
		for (int pageID : pageIds) {
			buf.killPage(pageID, file);
		}
	}

	public void print(Transaction tr, DBLock lock) throws IOException,
			InterruptedException {
		List<Node> items = new ArrayList<>();
		items.add(root);
		while (!items.isEmpty()) {
			List<Node> children = new ArrayList<>();
			for (Node<?> node : items) {
				final Collection<Node> print = node.print(tr, lock);
				if (print != null) children.addAll(print);
			}
			System.out.println();
			items = children;
		}
	}

	public void lockPath(Transaction tr, K key, DBLock el, Map<K, T> sm)
			throws IOException, InterruptedException {
		Integer indexPage = getRootPageId();
		while (indexPage != null) {
			indexPage = getNextPageIdToLock(indexPage, key, sm, tr, el);
		}
	}

	// helpers for locking
	/**
	 * Returns the next page id of the index we need to lock in our search for
	 * the given key. When the grantedPage is a leaf node this function returns
	 * null to signal we locked all the path to the key while at the same time
	 * populating the sm output parameter with the map of keys and page ids in
	 * the leaf node.
	 *
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private Integer getNextPageIdToLock(int toLock, K key, Map<K, T> m,
			Transaction tr, DBLock lock) throws IOException,
			InterruptedException {
		Node<?> node = root.newNodeFromDiskOrBuffer(tr, lock, toLock);
		if (node.isLeaf()) {
			@SuppressWarnings("unchecked")
			// it is a Leaf Node
			LeafNode leaf = (LeafNode) node;
			m.put(key, leaf._get(key));
			return null; // locked the path to the key
		}
		@SuppressWarnings("unchecked")
		// node is not leaf here
		InternalNode in = (InternalNode) node;
		final Node<?> nextNode = in._lookup(tr, lock, key);
		return nextNode.getPageId();
	}

	/** Return a page id for the root node */
	private synchronized int getRootPageId() {
		return root.getPageId();
	}

	// =========================================================================
	// Nodes
	// =========================================================================
	@SuppressWarnings("synthetic-access")
	public abstract class Node<V> extends RecordsPage<K, V> {

		// MUTABLE STATE
		volatile short numOfKeys; // NEVER ZERO EXCEPT ON CONSTRUCTION
		// CONSTANTS
		private static final short LEAF_OFFSET = 0;
		private static final short HEADER_SIZE = 3; // 1 isLeaf, 2 numOfKeys
		// PROTECTED
		protected static final short NUM_KEYS_OFFSET = 1;
		// FINALS
		private final boolean isLeaf;

		/**
		 * Instantiates a Node by loading a page from disk and sets its fields
		 * by reading the relevant info from the page. Called only from
		 * {@link #newNodeFromDiskOrBuffer(Transaction, DBLock, int)} where
		 * there is a transaction (and locking) OR in the tree constructor where
		 * no locking is needed. So we do not request locks here.
		 */
		private Node(int id, Serializer<V> ser) throws IOException,
				InterruptedException {
			super(buf.allocFrame(id, file), serKey, ser, HEADER_SIZE);
			isLeaf = readByte(LEAF_OFFSET) == 1;
			numOfKeys = readShort(NUM_KEYS_OFFSET);
		}

		/** Allocates a Node IN MEMORY */
		private Node(Transaction tr, boolean leaf, Serializer<V> ser)
				throws InterruptedException {
			super(buf.allocFrameForNewPage(nodeId.decrementAndGet()), serKey,
					ser, HEADER_SIZE);
			if (tr != null) { // if null we are creating the first root !
				final int id = getPageId();
				if (tr.lock(id, DBLock.E)) { // should always return true //
					// notice the lock is for WRITING !
					buf.pinPage(id);
				}
			}
			isLeaf = leaf;
			writeByte(LEAF_OFFSET, (byte) ((leaf) ? 1 : 0));
			numOfKeys = 0; // Unneeded
			writeShort(NUM_KEYS_OFFSET, (short) 0);
			buf.setPageDirty(getPageId());
		}

				Node<?>
				newNodeFromDiskOrBuffer(Transaction tr, DBLock lock, int pageID)
						throws IOException, InterruptedException {
			Page p = alloc(tr, lock, pageID);
			boolean leaf = p.readByte(LEAF_OFFSET) == 1;
			if (leaf) return new LeafNode(pageID);
			return new InternalNode(pageID);
		}

		// =====================================================================
		// Abstract
		// =====================================================================
		/**
		 * Return the Leaf that should contain key k starting from this Node.
		 * Locks the path up to the leaf.
		 */
		abstract LeafNode findLeaf(Transaction tr, DBLock e, K k)
				throws IOException, InterruptedException;

		/**
		 * Do not call *on* tree root. For (non tree root) Node n,
		 * n.parent(tree_root) will return the parent of n. tree_root should be
		 * Internal unless the only node in the tree.
		 *
		 * @throws InterruptedException
		 * @throws IOException
		 * @throws ClassCastException
		 *             if no parent found (tries to find child nodes of leafs)
		 */
		abstract InternalNode parent(Transaction tr, DBLock lock,
				InternalNode candidateParent) throws IOException,
				InterruptedException;

		abstract Collection<Node> print(Transaction tr, DBLock lock)
				throws IOException, InterruptedException;

		// =====================================================================
		// Node methods
		// =====================================================================
		final boolean overflow() { // TODO belongs to record page
			return numOfKeys == getMax_keys(); // no more keys accepted
		}

		/**
		 * Returns true if removing from this node WILL result in underflow. For
		 * nodes whose max keys number is even this will occur when the current
		 * number of keys drops below max_keys/2 (no rounding). For nodes whose
		 * max keys number is odd the current number of nodes must be
		 * floor[max_keys/2] - the fanout is max_keys + 1 and the last pointer
		 * is always present. Root will underflow if current fanout is 2 - and
		 * since the "greaterOrEqual" node is always there this means
		 * numOfNodes==1.
		 */
		final boolean willUnderflow() {
			if (root.equals(this)) return numOfKeys == 1;
			return numOfKeys == getMax_keys() / 2;
		}

		final boolean isLeaf() {
			return isLeaf;
		}

		final int greaterOrEqual() { // 4 for int - grOrE is always a page Id
			return readInt(Engine.PAGE_SIZE - 4);
		}

		final void setGreaterOrEqual(int integer) {
			writeInt(Engine.PAGE_SIZE - 4, integer);
			buf.setPageDirty(getPageId());
		}

		// =====================================================================
		// Methods that go to a Page subclass for Sorted Data files TODO
		// =====================================================================
		// final int MEDIAN_KEY_INDEX = (numOfKeys) / 2;
		void _put(Record<K, V> record) {
			_put(record.getKey(), record.getValue());
		}

		/** Removes {@code key} if it exists */
		void _remove(K key) {
			K tmpKey = readKey(0); // first key
			for (short i = 0; i < numOfKeys - 1; ++i) {
				final K nextKey = readKey(i + 1);
				if (key.compareTo(tmpKey) == 0) {
					writeKey(i, nextKey);
					writeValue(i, readValue(i + 1));
					key = nextKey;
				}
				tmpKey = nextKey;
			} // if key not found return false
			--numOfKeys;
			writeShort(NUM_KEYS_OFFSET, numOfKeys);
			buf.setPageDirty(getPageId());
		}

		/**
		 * Inserts key and value and increases numOfKeys. If key exists replaces
		 * its value with {@code value} and does not increase key count. Caller
		 * must ensure that space exists in the page or an
		 * IndexOutOfBoundsException is thrown.
		 */
		void _put(K k, V v) {
			for (short i = 0; i < numOfKeys; ++i) {
				K tmpKey = readKey(i);
				V tmpValue = readValue(i);
				if (k.compareTo(tmpKey) < 0) {
					writeKey(i, k);
					writeValue(i, v);
					k = tmpKey;
					v = tmpValue;
				} else if (k.compareTo(tmpKey) == 0) {
					writeValue(i, v);
					buf.setPageDirty(getPageId());
					return; // replace the value and do NOT ++numOfKeys
				}
			}
			writeKey(numOfKeys, k);
			writeValue(numOfKeys, v);
			++numOfKeys;
			writeShort(NUM_KEYS_OFFSET, numOfKeys);
			buf.setPageDirty(getPageId());
		}

		/**
		 * Returns the value with key {@code key} or {@code null} if no such key
		 * exists.
		 */
		V _get(K k) {
			for (short i = 0; i < numOfKeys; ++i) {
				if (k.compareTo(readKey(i)) == 0) return readValue(i);
			}
			return null;
		}

		K _lastKey() {
			return readKey(numOfKeys - 1);
		}

		K _firstKey() {
			return readKey(0);
		}

		Record<K, V> _lastPair() {
			return new Record<>(readKey(numOfKeys - 1),
				readValue(numOfKeys - 1));
		}

		Record<K, V> _firstPair() {
			return new Record<>(readKey(0), readValue(0));
		}

		void _copyTailAndRemoveIt(Node sibling, final int fromIndex) {
			if (fromIndex < 0) throw new IndexOutOfBoundsException();
			short removals = 0;
			for (int i = fromIndex; i < numOfKeys; ++i) {
				sibling._put(readKey(i), readValue(i)); // marks sibling dirty
				++removals;
			}
			numOfKeys -= removals;
			writeShort(NUM_KEYS_OFFSET, numOfKeys);
			buf.setPageDirty(getPageId());
		}

		// TODO assert this.parent == parent - Move to subclasses node so they
		// return a subclass
		Node<?> _rightSiblingSameParent(Transaction tr, DBLock lock,
				InternalNode parent) throws IOException, InterruptedException {
			if (parent == null)
				throw new NullPointerException("Root siblings ?");
			if (_firstKey().compareTo(parent._lastKey()) >= 0) // ...
				return null; // it is the "greater or equal" child (rightmost)
			final K lastKey = _lastKey(); // ... debugger - the last key may be
			// deleted
			for (short i = 0; // up to parent's penultimate key
			i < parent.numOfKeys - 1; ++i) {
				K parKey = parent.readKey(i);
				if (lastKey.compareTo(parKey) < 0)
					return newNodeFromDiskOrBuffer(tr, lock,
						parent.readValue(i + 1));
			}
			// this is the last key so return the "greater or equal"
			return newNodeFromDiskOrBuffer(tr, lock, parent.greaterOrEqual());
		}

		// TODO assert this.parent == parent - Move to subclasses node so they
		// return a subclass
		Node<?> _leftSiblingSameParent(Transaction tr, DBLock lock,
				InternalNode parent) throws IOException, InterruptedException {
			if (parent == null)
				throw new NullPointerException("Root siblings ?");
			final K _lastKey = _lastKey();
			if (_lastKey.compareTo(parent._firstKey()) < 0) // ...
				return null; // it is the leftmost child
			for (short i = 1; i < parent.numOfKeys; ++i) {
				K readKey = parent.readKey(i);
				if (_lastKey.compareTo(readKey) < 0)
					return newNodeFromDiskOrBuffer(tr, lock,
						parent.readValue(i - 1));
			}
			return newNodeFromDiskOrBuffer(tr, lock, parent._lastPair()
				.getValue());
		}
	}

	@SuppressWarnings("synthetic-access")
	final class InternalNode extends Node<Integer> {

		/**
		 * Used in {@link #split(Record)} and when the tree grows (the root
		 * splits, {@link BPlusDisk#insertInternal(Node, Record)}).
		 */
		InternalNode(Transaction tr) throws InterruptedException {
			super(tr, false, IntegerSerializer.INSTANCE);
		}

		InternalNode(int id) throws IOException, InterruptedException {
			super(id, IntegerSerializer.INSTANCE);
		}

		// =====================================================================
		// Overrides
		// =====================================================================
		@Override
		InternalNode parent(Transaction tr, DBLock lock, InternalNode root1)
				throws IOException, InterruptedException {
			final int id = getPageId();
			if (id == root1.greaterOrEqual()) return root1;
			for (short i = 0; i < root1.numOfKeys; ++i) {
				if (id == root1.readValue(i)) return root1;
			}
			return parent(tr, lock,
				(InternalNode) root1._lookup(tr, lock, this)); // the CCE
		}

		@Override
		LeafNode findLeaf(Transaction tr, DBLock lock, K k) throws IOException,
				InterruptedException {
			return _lookup(tr, lock, k).findLeaf(tr, lock, k);
		}

		@Override
		Collection<Node> print(Transaction tr, DBLock lock) throws IOException,
				InterruptedException {
			System.out.print(getPageId() + "::");
			Collection<Node> values = new ArrayList<>();
			for (short i = 0; i < numOfKeys; ++i) {
				K key = readKey(i);
				int val = readValue(i);
				values.add(newNodeFromDiskOrBuffer(tr, lock, val));
				System.out.print(key + ";" + val + ",");
			}
			System.out.print(greaterOrEqual() + "\t");
			values.add(newNodeFromDiskOrBuffer(tr, lock, greaterOrEqual()));
			return values;
		}

		// =====================================================================
		// Class Methods
		// =====================================================================
		Node<?> _lookup(Transaction tr, DBLock lock, final K key)
				throws IOException, InterruptedException {
			if (key.compareTo(_lastKey()) >= 0)
				return newNodeFromDiskOrBuffer(tr, lock, greaterOrEqual());
			// tailMap contains at least children.lastKey()
			for (short i = 0; i < numOfKeys; ++i) {
				K readKey = readKey(i);
				if (key.compareTo(readKey) < 0)
					return newNodeFromDiskOrBuffer(tr, lock, readValue(i));
			}
			throw new RuntimeException("key " + key + " not found"); // TODO
																		// keep?
		}

		/**
		 * Wrappers around _lookup(K k) - look a node up means look its lastKey
		 * up
		 */
		private Node<?> _lookup(Transaction tr, DBLock lock,
				InternalNode internalNode) throws IOException,
				InterruptedException {
			return _lookup(tr, lock, internalNode._lastKey());
		}

		/**
		 * Returns the key for this node or null if this node is the
		 * "greater or equal" child or not a child
		 */
		private K _keyWithValue(Node<?> anchor) {
			final int id = anchor.getPageId();
			for (short i = 0; i < numOfKeys; ++i) {
				int readVal = readValue(i);
				if (id == readVal) return readKey(i);
			}
			/* if (greaterOrEqual().equals(id)) */return null; // No key for
			// this node TODO make _child method
		}

		Record<K, Node> split(Transaction tr, Record<K, Node> insert)
				throws InterruptedException {
			if (numOfKeys != getMax_keys())
				throw new RuntimeException("Splitting internal node - keys: "
					+ numOfKeys);
			System.out.println("------------------> SPLIT INTERNAL NODE");
			// this key to insert must point to the just split node
			final K keyToInsert = insert.getKey();
			// splitting an internal node means we need to point to the
			// node that was just split - the new node is already inserted !
			final Node<?> justSplit = insert.getValue();
			final InternalNode sibling = new InternalNode(tr);
			sibling.setGreaterOrEqual(greaterOrEqual()); // sure
			_copyTailAndRemoveIt(sibling, (numOfKeys + 1) / 2); // do NOT copy
			// the median in case max_keys is odd
			// numOfKeys = (short) ((numOfKeys - 1) / 2 + 1); // draw 2 pictures
			// - for max_keys == odd and max_keys == even
			if (keyToInsert.compareTo(_lastKey()) < 0) {
				_put(keyToInsert, justSplit.getPageId());
				// insert it and move last key to the sibling
				sibling._put(_lastPair());
				--numOfKeys;
			} else {
				sibling._put(new Record<>(keyToInsert, justSplit.getPageId()));
			}
			Record<K, Integer> _lastPair = _lastPair();
			writeShort(NUM_KEYS_OFFSET, --numOfKeys); // discard _lastPair
			buf.setPageDirty(getPageId());
			setGreaterOrEqual(_lastPair.getValue());
			return new Record<K, Node>(_lastPair.getKey(), sibling);
		}

		Record<K, Node> insertInternal(Transaction tr, Node justSplit,
				Record<K, Node> insert) throws InterruptedException {
			final Node<?> newNode = insert.getValue();
			K _keyOfAnchor = _keyWithValue(justSplit);
			if (_keyOfAnchor != null) {
				_put(_keyOfAnchor, newNode.getPageId());
			} else {
				// _keyOfAnchor == null - anchor used to be for keys greater or
				// equal to lastKey
				setGreaterOrEqual(newNode.getPageId());
			}
			if (overflow()) {// split
				return split(tr, new Record<>(insert.getKey(), justSplit));
			}
			_put(insert.getKey(), justSplit.getPageId());
			return null;
		}

		public <L extends Node<?>> Record<K, InternalNode> removeInternal(
				Transaction tr, Node<?> merged, Record<K, L> merge)
				throws IOException, InterruptedException {
			K newKey = merge.getKey();
			if (newKey != null) {
				// no merging took place but we need to update the keys
				final Node<?> reKeyed = merge.getValue();
				K rekeyedNodeKey = _keyWithValue(reKeyed);
				if (rekeyedNodeKey == null) { // the rekeyed node was the grOrEq
					// we need to point from the key given to the merged
					// UNTESTED IN MY SCENARIO reKeyed is left or me when I have
					// a right sibling (see LeafNode#merge)
					K key3 = _keyWithValue(merged);
					_remove(key3);
					_put(newKey, merged.getPageId());
					return null;
				}
				_remove(rekeyedNodeKey);
				_put(newKey, reKeyed.getPageId());
				return null;
			}
			// newKey == null - a node was actually deleted
			final Node<?> deleted = merge.getValue();
			K keyDeleted = _keyWithValue(deleted); // will be finally deleted
			final K keyMergedNode = _keyWithValue(merged);
			if (keyDeleted == null) { // we must replace the greaterOrEqual with
				// merged - BUT FIND merged first
				// we deleted the right sibling of the leaf OR THE LEAF ITSELF
				// IF IT WAS RIGHTMOST and it happened to be the greaterOrEqual
				// of its parent (this) - sooo:
				if (merged.equals(deleted)) {
					Record<K, Integer> _lastPair = _lastPair();
					this.setGreaterOrEqual(_lastPair.getValue());
					keyDeleted = _lastPair.getKey();
				} else {
					this.setGreaterOrEqual(merged.getPageId());
					// then we must remove the key that pointed to us (exists)
					keyDeleted = keyMergedNode;
				}
			} else {
				if (merged.equals(deleted)) {
					// we merged with our left sibling
					for (short i = 1; i < numOfKeys; ++i) {
						K readKey = readKey(i);
						if (keyDeleted.compareTo(readKey) < 0) {
							_put(keyDeleted, readValue(i - 1));
							keyDeleted = readKey(i - 1);
							break;
						}
					}
				} else {
					_put(keyDeleted, merged.getPageId());
					keyDeleted = keyMergedNode;
				}
			}
			// WE ARE NO ROOT - we are only called from fix internal which
			// checks this
			if (willUnderflow()) {
				_remove(keyDeleted);
				return merge(tr);
			}
			_remove(keyDeleted);
			return null;
		}

		private Record<K, InternalNode> merge(Transaction tr)
				throws IOException, InterruptedException {
			if (root.equals(this))
				throw new RuntimeException("Called merge on root");
			System.out.println("------------------> IN MERGE INTERNAL NODE");
			@SuppressWarnings("unchecked")
			// if this is not the root then the root must be internal node
			InternalNode parent = parent(tr, DBLock.E, (InternalNode) root);
			InternalNode right_sibling = (InternalNode) _rightSiblingSameParent(
				tr, DBLock.E, parent);
			InternalNode left_sibling = (InternalNode) _leftSiblingSameParent(
				tr, DBLock.E, parent);
			// DIFFERENCE WITH LEAF NODES - grOrEq changes !!!
			final InternalNode DAS = this;
			if ((left_sibling == null || left_sibling.willUnderflow())
				&& (right_sibling == null || right_sibling.willUnderflow())) {
				System.out.println("------------------> MERGING INTERNAL NODE");
				// FIXME - I always merge to my left node (while I split adding
				// more nodes on the right one when there is a choice) - does it
				// matter ? I merge to the left so I do not have to update the
				// "greater or equal" pointer (the pointer to the next leaf)
				// if matters FIXME - UPDATE THE INDEX ON THE LEFT SIBLING
				if (right_sibling != null) {
					// delete it
					int greaterOrEqual = greaterOrEqual();
					K thisKey = parent._keyWithValue(this);// this not rightmost
					_put(thisKey, greaterOrEqual);
					right_sibling._copyTailAndRemoveIt(DAS, 0);
					setGreaterOrEqual(right_sibling.greaterOrEqual());
					return new Record<>(null, right_sibling);
				}
				// both null == we are root - impossible
				// DELETE OURSELVES so we don't have to update "next" pointer of
				// our left left sibling
				// WE ARE RIGHTMOST (right_sibling == null)
				int thisGreater = left_sibling.greaterOrEqual();
				K leftKey = parent._keyWithValue(left_sibling); // exists
				left_sibling._put(leftKey, thisGreater);
				this._copyTailAndRemoveIt(left_sibling, 0);
				left_sibling.setGreaterOrEqual(this.greaterOrEqual());
				return new Record<>(null, DAS);
			}
			// AT LEAST ONE SIBLING WITH EXTRA NODES
			// PREFER THE LEFT ONE so the right one has more records as in split
			// FIXME - opposite to merge (see FIXMEs above)
			if (left_sibling != null && !left_sibling.willUnderflow()) {
				// just prevent the underflow - copy ONE node FIXME ALGORITHM
				// DIFFERENCE WITH LEAF NODES - grOrEq changes !!!
				Record<K, Integer> lastPair = left_sibling._lastPair();
				int greaterOrEqual = left_sibling.greaterOrEqual();
				left_sibling.setGreaterOrEqual(lastPair.getValue());
				// left sibling - not gOrEq // Common parent !
				K siblingKeyInParent = parent._keyWithValue(left_sibling);
				K key = lastPair.getKey();
				left_sibling._remove(key);
				_put(siblingKeyInParent, greaterOrEqual);
				// we need to change in the parent the key pointing to this node
				// with the ex first key of the right sibling
				return new Record<>(key, left_sibling);
			}
			// just prevent the underflow - copy ONE node FIXME ALGORITHM
			Record<K, Integer> firstPair = right_sibling._firstPair();
			int greaterOrEqual = DAS.greaterOrEqual();
			DAS.setGreaterOrEqual(firstPair.getValue());
			// WE ARE left sibling - not gOrEq // Common parent !
			K thisKeyInParent = parent._keyWithValue(DAS);
			K key = firstPair.getKey();
			right_sibling._remove(key);
			_put(thisKeyInParent, greaterOrEqual);
			// we need to change in the parent the key pointing to this node
			// with the ex first key of the right sibling
			return new Record<>(key, DAS);
		}
	}

	@SuppressWarnings("synthetic-access")
	final class LeafNode extends Node<T> {

		/**
		 * Used in {@link #split(Record)} and in creating the tree for the first
		 * time (see {@link BPlusDisk#BPlusDisk(IndexDiskFile, short, short)}).
		 * In the latter case the transaction must be null.
		 */
		LeafNode(Transaction tr) throws InterruptedException {
			super(tr, true, serVal);
		}

		LeafNode(int id) throws InterruptedException, IOException {
			super(id, serVal);
		}

		// =========================================================================
		// Overrides
		// =========================================================================
		@Override
		InternalNode parent(Transaction tr, DBLock lock, InternalNode root1)
				throws IOException, InterruptedException {
			final int id = getPageId();
			if (root1._keyWithValue(this) != null
				|| id == root1.greaterOrEqual()) return root1;
			return parent(tr, lock, // the CCE will be thrown from this cast
				(InternalNode) root1._lookup(tr, lock, _firstPair().getKey()));
		}

		@Override
		LeafNode findLeaf(Transaction tr, DBLock lock, K k) {
			return this;
		}

		@Override
		Collection<Node> print(Transaction tr, DBLock lock) {
			System.out.print(getPageId() + ":"/* + numOfKeys */+ ":");
			for (short i = 0; i < numOfKeys; ++i) {
				K key = readKey(i);
				T val = readValue(i);
				System.out.print(key + ";" + val + ",");
			}
			System.out.print(greaterOrEqual() + "\t");
			return null;
		}

		// =====================================================================
		// Class Methods
		// =====================================================================
		Record<K, Node> insertInLeaf(Transaction tr, Record<K, T> rec)
				throws InterruptedException {
			if (overflow()) return split(tr, rec);
			_put(rec.getKey(), rec.getValue());
			return null;
		}

		Record<K, LeafNode> deleteInLeaf(Transaction tr, K rec)
				throws InterruptedException, IOException { // TODO why IO
			// we are in leaf - if the leaf is root then it is the only node
			if (root.equals(this)) {
				_remove(rec);
				return null;
			}
			// WE ARE NO ROOT, there are internal nodes
			if (willUnderflow()) {
				_remove(rec);
				return merge(tr);
			}
			_remove(rec);
			return null;
		}

		/** Must not be called on the root (if the root is leaf) */
		Record<K, LeafNode> merge(Transaction tr) throws InterruptedException,
				IOException {
			if (root.equals(this))
				throw new RuntimeException("Called merge on root");
			System.out.println("------------------> IN MERGE LEAFNODE");
			// TODO - the part below is common with internal node - refactor
			@SuppressWarnings("unchecked")
			// if this is not the root then the root must be internal node
			InternalNode parent = parent(tr, DBLock.E, (InternalNode) root);
			LeafNode right_sibling = (LeafNode) _rightSiblingSameParent(tr,
				DBLock.E, parent);
			LeafNode left_sibling = (LeafNode) _leftSiblingSameParent(tr,
				DBLock.E, parent);
			final LeafNode DAS = this;
			if ((left_sibling == null || left_sibling.willUnderflow())
				&& (right_sibling == null || right_sibling.willUnderflow())) {
				// I HAVE TO MERGE - return a record with the deleted node
				System.out.println("------------------> MERGING LEAFNODES");
				// FIXME - I always merge to my left node (while I split adding
				// more nodes on the right one when there is a choice) - does it
				// matter ? I merge to the left so I do not have to update the
				// "greater or equal" pointer (the pointer to the next leaf)
				// if matters FIXME - UPDATE THE INDEX ON THE LEFT SIBLING
				if (right_sibling != null) {
					// delete it
					right_sibling._copyTailAndRemoveIt(DAS, 0); // TODO leaves
					// the page in the index file with 0 numOfKeys - compact the
					// index file
					setGreaterOrEqual(right_sibling.greaterOrEqual());
					return new Record<>(null, right_sibling);
				}
				// both null == we are root - impossible
				// DELETE OURSELVES so we don't have to update "next" pointer of
				// our left left sibling
				// WE ARE RIGHTMOST (right_sibling == null)
				DAS._copyTailAndRemoveIt(left_sibling, 0);
				left_sibling.setGreaterOrEqual(DAS.greaterOrEqual());
				return new Record<>(null, DAS);
			}
			// AT LEAST ONE SIBLING WITH EXTRA NODES - return a record with the
			// new key for _this_ or the left_sibling
			// PREFER THE LEFT ONE so the right one has more records as in split
			// FIXME - opposite to merge (see FIXMEs above)
			if (left_sibling != null && !left_sibling.willUnderflow()) {
				// just prevent the underflow - copy ONE node FIXME ALGORITHM
				Record<K, T> lastPair = left_sibling._lastPair();
				K key = lastPair.getKey();
				left_sibling._remove(key);
				_put(lastPair);
				// we need to change in the parent the key pointing to this node
				// with the NEW first key of the right sibling
				return new Record<>(key, left_sibling);
			}
			// just prevent the underflow - copy ONE node FIXME ALGORITHM
			Record<K, T> firstPair = right_sibling._firstPair();
			K key = firstPair.getKey();
			right_sibling._remove(key);
			_put(firstPair);
			// we need to change in the parent the key pointing to this node
			// with the ***NEW*** first key of the right sibling
			K keyNew = right_sibling._firstKey();
			return new Record<>(keyNew, DAS);
		}

		Record<K, Node> split(Transaction tr, Record<K, T> rec)
				throws InterruptedException {
			if (numOfKeys != getMax_keys())
				throw new RuntimeException("Splitting leaf - keys: "
					+ numOfKeys);
			System.out.println("------------------> SPLIT LEAFNODE");
			LeafNode sibling = new LeafNode(tr);
			sibling.setGreaterOrEqual(greaterOrEqual());
			setGreaterOrEqual(sibling.getPageId());
			// FIXME ALGORITHM
			// move median and up to sibling
			_copyTailAndRemoveIt(sibling, numOfKeys / 2); // ...
			// insert
			if (rec.getKey().compareTo(_lastKey()) < 0) {
				_put(rec.getKey(), rec.getValue());// insert in this
				sibling._put(_lastPair());
				--numOfKeys;
			} else {
				sibling._put(rec.getKey(), rec.getValue());
			}
			writeShort(NUM_KEYS_OFFSET, numOfKeys);
			buf.setPageDirty(getPageId());
			return new Record<K, Node>(sibling._firstPair().getKey(), sibling);
		}
	}

	// =========================================================================
	// Helpers
	// =========================================================================
	private void insertInternal(Transaction tr, Node<?> justSplit,
			Record<K, Node> insert) throws InterruptedException, IOException {
		if (root.equals(justSplit)) { // root must split (leaf or not)
			System.out.println("------------------> SPLIT ROOT");
			InternalNode newRoot = new InternalNode(tr);
			newRoot._put(insert.getKey(), justSplit.getPageId());
			newRoot.setGreaterOrEqual(insert.getValue().getPageId());
			setRoot(newRoot);
			return;
		}
		// justSplit is not tree root so has a parent
		@SuppressWarnings("unchecked")
		// moreover root is not leaf so I cast it to InternalNode safely
		InternalNode parent = justSplit.parent(tr, DBLock.E,
			(InternalNode) root);
		Record<K, Node> newInternalNode = parent.insertInternal(tr, justSplit,
			insert);
		if (newInternalNode != null) // RECURSION
			insertInternal(tr, parent, newInternalNode);
	}

	private synchronized void setRoot(Node newRoot) {
		root = newRoot;
	}

	/**
	 * Inserts the record in the leaf - if it exists throws
	 * IllegalArgumentException. Must be called AFTER I lock the path to the
	 * leaf for writing.
	 */
	private <R extends Record<K, T>> void _insertInLeaf(Transaction tr, R rec,
			final LeafNode leafNode) throws IllegalArgumentException,
			InterruptedException, IOException {
		if (leafNode._get(rec.getKey()) != null)
			throw new IllegalArgumentException("Key " + rec.getKey()
				+ " exists");
		Record<K, Node> insert = leafNode.insertInLeaf(tr, rec);
		if (insert != null) { // got a key back, so leafNode split
			insertInternal(tr, leafNode, insert); // all parents are locked
		}
	}

	/**
	 * Removes the key from the leaf - if it does not exist throws
	 * IllegalArgumentException. Must be called AFTER I lock the path to the
	 * leaf for writing.
	 */
	private void _deleteInLeaf(Transaction tr, K key, final LeafNode leafNode)
			throws IllegalArgumentException, InterruptedException, IOException {
		if (leafNode._get(key) == null)
			throw new IllegalArgumentException("Key " + key + " does not exist");
		Record<K, LeafNode> merge = leafNode.deleteInLeaf(tr, key);
		if (merge != null) { // leafNode split
			fixInternal(tr, leafNode, merge); // all parents are NOT locked
			// (SEE FIXME in merge())
		}
	}

	@SuppressWarnings("synthetic-access")
	private <L extends Node<?>> void fixInternal(Transaction tr,
			Node<?> merged, Record<K, L> merge) throws IOException,
			InterruptedException {
		@SuppressWarnings("unchecked")
		// SW: if root were leaf it would be the only node so delete in leaf
		// would have not ever called fixInternal
		final InternalNode das_root = (InternalNode) root;
		// *********** get our parent to see if it is root
		K newKey = merge.getKey();
		// merged and deleted (if a deletion occurred) parent should be the same
		InternalNode parent = merged.parent(tr, DBLock.E, das_root);
		// *********** if parent is root perform what's needed // TODO MOVE THIS
		// in removeInternal()
		if (root.equals(parent)) {
			System.out.println("------------------> FIX ROOT");
			if (newKey != null) {
				// no merging took place but we need to update the keys
				final Node<?> reKeyed = merge.getValue();
				K rekeyedNodeKey = (das_root)._keyWithValue(reKeyed);
				if (rekeyedNodeKey == null) { // the rekeyed node was the grOrEq
					// we need to point from the key given to the merged
					K key3 = (das_root)._keyWithValue(merged);
					root._remove(key3);
					das_root._put(newKey, merged.getPageId());
					return; // TODO test this path
				}
				root._remove(rekeyedNodeKey);
				das_root._put(newKey, reKeyed.getPageId());
				return;
			}
			// newKey == null - a node was actually deleted
			final Node<?> deleted = merge.getValue();
			K key = (das_root)._keyWithValue(deleted);
			final K keyMergedNode = das_root._keyWithValue(merged);
			if (key == null) {
				if (merged.equals(deleted)) {
					// keyMergedNode == key == null
					Record<K, Integer> _lastPair = das_root._lastPair();
					root.setGreaterOrEqual(_lastPair.getValue());
					key = _lastPair.getKey();
				} else {
					root.setGreaterOrEqual(merged.getPageId());
					// then we must remove the key that pointed to us (exists)
					key = keyMergedNode;
				}
			} else {
				if (merged.equals(deleted)) {
					for (short i = 1; i < root.numOfKeys; ++i) {
						K readKey = root.readKey(i);
						if (key.compareTo(readKey) < 0) {
							das_root._put(key, das_root.readValue(i - 1));
							break;
						}
					}
				} else {
					das_root._put(key, merged.getPageId());
					key = keyMergedNode;
				}
			}
			if (root.numOfKeys == 1) {
				System.out.println("------------------> DELETE ROOT");
				int _get = das_root._get(key);
				root._remove(key); // to mark it --numOfKeys
				setRoot(root.newNodeFromDiskOrBuffer(tr, DBLock.E, _get));
				return;
			}
			root._remove(key);
			return;
		}
		// *********** parent is not root
		Record<K, InternalNode> newInternalNode = parent.removeInternal(tr,
			merged, merge);
		if (newInternalNode != null) // RECURSION
			fixInternal(tr, parent, newInternalNode);
	}

	private Page alloc(Transaction tr, DBLock lock, int pageID)
			throws IOException, InterruptedException {
		Page p;
		if (tr.lock(pageID, lock)) {
			p = buf.allocFrame(pageID, file);
			// FIXME - race in pin ??? - add boolean pin param in allocFrame
			buf.pinPage(pageID);
		} else {
			p = buf.allocFrame(pageID, file);
		}
		return p;
	}
}
