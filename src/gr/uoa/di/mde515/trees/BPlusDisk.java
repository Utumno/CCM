package gr.uoa.di.mde515.trees;

import gr.uoa.di.mde515.engine.Engine;
import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.engine.buffer.BufferManager;
import gr.uoa.di.mde515.engine.buffer.Page;
import gr.uoa.di.mde515.files.IndexDiskFile;
import gr.uoa.di.mde515.index.PageId;
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
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Disk resident B plus tree. When a node that accepts an EVEN number of items
 * splits the middle node is moved to the RIGHT sibling. For instance (for max
 * number of items equal to 4 (N=2, 2*N + 1 = 5)):
 *
 * 12345 -> 12 345
 *
 * Stores Integers as keys.
 *
 * @param <V>
 *            the type of the value of the records to be stored in the leaf
 *            nodes - when the tree is used as an Index this corresponds to the
 *            page id of the data file the key is located
 */
public final class BPlusDisk<V> {

	private static final BufferManager<Integer> buf = BufferManager
		.getInstance();
	private final IndexDiskFile file;
	// fields
	private Node root;
	private AtomicInteger nodeId = new AtomicInteger(0);
	// sizes of the key and value to be used in the tree
	private final short key_size;
	@SuppressWarnings("unused")
	private final short value_size;
	private final short record_size;

	static final class Root { // serialization proxy really

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

	public BPlusDisk(IndexDiskFile file, short key_size, short value_size)
			throws IOException, InterruptedException {
		this.key_size = key_size;
		this.value_size = value_size;
		this.record_size = (short) (key_size + value_size);
		// this.diskRec = diskRec;
		this.file = file;
		if (file.read() != -1) {
			System.out.println(file + " already exists");
			nodeId.set(Root.nodesFromFile());
			int rootFromFile = Root.rootFromFile();
			Page<Integer> allocFrame = buf.allocFrame(rootFromFile, file);
			boolean leaf = allocFrame.readByte(Node.LEAF_OFFSET) == 1;
			root = (leaf) ? new LeafNode(rootFromFile) : new InternalNode(
				rootFromFile);
			// FIXME permanent alloc (and something else I forgot ...)
		} else { // FILE EMPTY - CREATE THE ROOT
			System.out.println(file + ": Creating...");
			root = new LeafNode(null); // null transaction !
			buf.setPageDirty(-1);
			buf.flushPage(-1, file); // TODO wild flush
		}
	}

	// =========================================================================
	// API
	// =========================================================================
	public void flush(List<PageId<Integer>> pageIds) throws IOException {
		for (PageId<Integer> pageID : pageIds) {
			final Integer pid = pageID.getId();
			buf.flushPage(pid, file);
			System.out.println("PID " + pid);
			buf.unpinPage(pid);
		}
		flushRootAndNodes();
	}

	public <R extends Record<Integer, Integer>> void insert(Transaction tr,
			R rec) throws IOException, InterruptedException {
		final Integer id = root.getPageId().getId();
		if (tr.lock(id, DBLock.E)) {
			buf.allocFrame(id, file);
			// FIXME - race in pin ??? - add boolean pin param in allocFrame
			buf.pinPage(id);
		} else {
			buf.allocFrame(id, file);
		}
		final LeafNode leafNode = root.findLeaf(tr, DBLock.E, rec.getKey());
		_insertInLeaf(tr, rec, leafNode);
	}

	/** Return a page id for the root node */
	public PageId<Integer> getRootPageId() {
		return root.getPageId();
	}

	public void flushRootAndNodes() throws IOException {
		Root.rootToFile(root.getPageId().getId());
		Root.nodesToFile(nodeId.get());
	}

	/**
	 * Returns the next page id of the index we need to lock in our search for
	 * the given key. When the grantedPage is a leaf node this function returns
	 * null to signal we locked all the path to the key while at the same time
	 * populating the sm output parameter with the map of keys and page ids in
	 * the leaf node.
	 *
	 * @param tr
	 * @param lock
	 *
	 * @throws InterruptedException
	 * @throws IOException
	 */
	// public PageId<Integer> getNextPageId(PageId<Integer> grantedPage,
	// Integer key,
	// SortedMap<Integer, Integer> sm, Transaction tr, DBLock lock)
	// throws IOException,
	// InterruptedException {
	// Node node = grantedPage.getId();
	// if (node.isLeaf()) {
	// sm.putAll(((LeafNode) node).records());
	// return null; // locked the path to the key
	// }
	// InternalNode in = (InternalNode) node;
	// final Node nextNode = in._lookup(tr, lock, key);
	// return new PageId<>(nextNode);
	// }
	//
	// public <R extends Record<Integer, Integer>> PageId<Node> getLeaf(
	// PageId<Node> grantedPage, R rec) throws IOException,
	// InterruptedException {
	// Node node = grantedPage.getId();
	// final Integer key = rec.getKey();
	// if (node.isLeaf()) {
	// _insertInLeaf(rec, (LeafNode) node); // all parents are
	// // locked
	// return null; // granted
	// }
	// InternalNode in = (InternalNode) node;
	// final Node nextNode = in._lookup(key);
	// return new PageId<>(nextNode);
	// }
	public void print(Transaction tr, DBLock lock) throws IOException,
			InterruptedException {
		List<Node> items = new ArrayList<>();
		items.add(root);
		while (!items.isEmpty()) {
			List<Node> children = new ArrayList<>();
			for (Node node : items) {
				final Collection<Node> print = node.print(tr, lock);
				if (print != null) children.addAll(print);
			}
			System.out.println();
			items = children;
		}
	}

	// =========================================================================
	// Nodes
	// =========================================================================
	@SuppressWarnings("synthetic-access")
	public abstract class Node extends Page<Integer> {

		short numOfKeys; // NEVER ZERO
		static final short HEADER_SIZE = 3; // 2 for numOfKeys, 1 for isLeaf
		private static final short LEAF_OFFSET = 0;
		protected static final short NUM_KEYS_OFFSET = 1;
		private final boolean isLeaf;
		private final short max_keys = (short) ((Engine.PAGE_SIZE - HEADER_SIZE - key_size) / record_size);

		synchronized short getMax_keys() { // AUTOGENERATED SYNCHRONIZED
			return max_keys;
		}

		/**
		 * Called only from
		 * {@link #newNodeFromDiskOrBuffer(Transaction, DBLock, int)} where
		 * there is a transaction (and locking) OR in the tree constructor where
		 * no locking is needed. So we do not request locks here.
		 */
		private Node(int id) throws IOException, InterruptedException {
			super(buf.allocFrame(id, file));
			isLeaf = readByte(LEAF_OFFSET) == 1;
			numOfKeys = readShort(NUM_KEYS_OFFSET);
		}

		/** Allocates a Node IN MEMORY */
		private Node(Transaction tr, boolean leaf) throws InterruptedException {
			super(buf.allocFrameForNewPage(nodeId.decrementAndGet()));
			if (tr != null) { // if null we are creating the first root !
				final Integer id = getPageId().getId();
				if (tr.lock(id, DBLock.E)) { // should always return true //
					// notice the lock is for WRITING !
					buf.pinPage(id);
				}
			}
			isLeaf = leaf;
			writeByte(LEAF_OFFSET, (byte) ((leaf) ? 1 : 0));
			numOfKeys = 0; // Unneeded
			writeShort(NUM_KEYS_OFFSET, (short) 0);
			buf.setPageDirty(this.getPageId().getId());
		}

		Node newNodeFromDiskOrBuffer(Transaction tr, DBLock lock, int pageID)
				throws IOException, InterruptedException {
			Page<Integer> p;
			if (tr.lock(pageID, lock)) {
				p = buf.allocFrame(pageID, file);
				// FIXME - race in pin ??? - add boolean pin param in allocFrame
				buf.pinPage(pageID);
			} else {
				p = buf.allocFrame(pageID, file);
			}
			boolean leaf = p.readByte(LEAF_OFFSET) == 1;
			if (leaf) return new LeafNode(pageID);
			return new InternalNode(pageID);
		}

		/**
		 * Return the Leaf that should contain key k starting from this Node.
		 * Locks the path up to the leaf.
		 */
		abstract LeafNode findLeaf(Transaction tr, DBLock e, Integer k)
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

		boolean overflow() {
			return numOfKeys == max_keys; // FIXME ............ Test
		}

		int items() {
			return numOfKeys;
		}

		boolean isLeaf() { // TODO UNUSED
			return isLeaf;
		}

		abstract Collection<Node> print(Transaction tr, DBLock lock)
				throws IOException, InterruptedException;

		@Override
		public String toString() {
			return "@" + getPageId().getId();
		}

		int greaterOrEqual() {
			return readInt(Engine.PAGE_SIZE - key_size);
		}

		void setGreaterOrEqual(int goe) {
			writeInt(Engine.PAGE_SIZE - key_size, goe);
			buf.setPageDirty(this.getPageId().getId());
		}

		// =====================================================================
		// Methods that go to a Page subclass for Sorted Data files TODO
		// =====================================================================
		// final int MEDIAN_KEY_INDEX = (numOfKeys) / 2;
		void _put(Record<Integer, Integer> rec) {
			_put(rec.getKey(), rec.getValue());
		}

		/**
		 * Inserts key and value and increases numOfKeys. If key exists replaces
		 * its value with {@code value} and does not increase key count.
		 */
		void _put(int key, int value) {
			int i, j;
			for (i = HEADER_SIZE, j = 0; j < numOfKeys; i += record_size, ++j) {
				int tmpKey = readInt(i);
				int tmpValue = readInt(i + key_size);
				if (key < tmpKey) {
					writeInt(i, key);
					writeInt(i + key_size, value);
					key = tmpKey;
					value = tmpValue;
				} else if (key == tmpKey) {
					writeInt(i + key_size, value);
					return; // replace the value and do NOT ++numOfKeys
				}
			}
			writeInt(i, key);
			writeInt(i + key_size, value);
			++numOfKeys;
			writeShort(NUM_KEYS_OFFSET, numOfKeys);
			buf.setPageDirty(this.getPageId().getId());
		}

		int _lastKey() {
			int offset = HEADER_SIZE + (numOfKeys - 1) * record_size;
			return readInt(offset);
		}

		Record<Integer, Integer> _lastPair() {
			int offset = HEADER_SIZE + (numOfKeys - 1) * record_size;
			return new Record<>(readInt(offset), readInt(offset + key_size));
		}

		Record<Integer, Integer> _firstPair() {
			int offset = HEADER_SIZE;
			return new Record<>(readInt(offset), readInt(offset + key_size));
		}

		void _copyTailAndRemoveIt(Node sibling, final int fromIndex) {
			short removals = 0;
			for (int j = fromIndex, i = HEADER_SIZE + j * record_size; j < numOfKeys; i += record_size, ++j) {
				sibling._put(readInt(i), readInt(i + key_size));
				++removals;
			}
			numOfKeys -= removals;
			writeShort(NUM_KEYS_OFFSET, numOfKeys);
			buf.setPageDirty(this.getPageId().getId());
		}
	}

	@SuppressWarnings("synthetic-access")
	final class InternalNode extends Node {

		/**
		 * Used in {@link #split(Record)} and when the tree grows (the root
		 * splits, {@link BPlusDisk#insertInternal(Node, Record)}).
		 */
		InternalNode(Transaction tr) throws InterruptedException {
			super(tr, false);
		}

		InternalNode(int id) throws IOException, InterruptedException {
			super(id);
		}

		Node _lookup(Transaction tr, DBLock lock, final Integer k)
				throws IOException, InterruptedException {
			if (k.compareTo(_lastKey()) >= 0)
				return newNodeFromDiskOrBuffer(tr, lock, greaterOrEqual());
			// tailMap contains at least children.lastKey()
			for (short i = HEADER_SIZE, j = 0; j < numOfKeys; i += record_size, ++j) {
				int readInt = readInt(i);
				if (k.compareTo(readInt) < 0)
					return newNodeFromDiskOrBuffer(tr, lock, readInt(i
						+ key_size));
			}
			throw new RuntimeException("key " + k + " not found"); // TODO keep?
		}

		/**
		 * Wrappers around _lookup(K k) - look a node up means look its lastKey
		 * up
		 */
		private Node _lookup(Transaction tr, DBLock lock,
				InternalNode internalNode) throws IOException,
				InterruptedException {
			return _lookup(tr, lock, internalNode._lastKey());
		}

		private Integer _keyWithValue(Node anchor) {
			final int id = anchor.getPageId().getId();
			for (short i = HEADER_SIZE, j = 0; j < numOfKeys; i += record_size, ++j) {
				int readInt = readInt(i + key_size);
				if (id == readInt) {
					return readInt(i);
				}
			}
			// if (greaterOrEqual.equals(anchor))
			// throw new RuntimeException("Node " + anchor
			// + " is not child of " + this); // TODO get back
			return null; // No key for this node
		}

		@Override
		InternalNode parent(Transaction tr, DBLock lock, InternalNode root1)
				throws IOException, InterruptedException {
			final int id = getPageId().getId();
			if (id == root1.greaterOrEqual()) return root1;
			for (short i = HEADER_SIZE, j = 0; j < numOfKeys; i += record_size, ++j) {
				if (id == root1.readInt(i)) return root1;
			}
			return parent(tr, lock,
				(InternalNode) root1._lookup(tr, lock, this)); // the CCE
		}

		@Override
		LeafNode findLeaf(Transaction tr, DBLock lock, Integer k)
				throws IOException, InterruptedException {
			return _lookup(tr, lock, k).findLeaf(tr, lock, k);
		}

		Record<Integer, Node>
				split(Transaction tr, Record<Integer, Node> insert)
						throws InterruptedException {
			if (numOfKeys != getMax_keys())
				throw new RuntimeException("Splitting non leaf node - keys: "
					+ numOfKeys);
			System.out.println("SPLIT INTERNAL NODE");
			// this key to insert must point to the just split node
			final int keyToInsert = insert.getKey();
			// splitting an internal node means we need to point to the
			// node that was just split - the new node was already inserted !
			final BPlusDisk<V>.Node justSplit = insert.getValue();
			// FIXME ALGORITHM
			final InternalNode sibling = new InternalNode(tr);
			sibling.setGreaterOrEqual(greaterOrEqual()); // sure
			_copyTailAndRemoveIt(sibling, (numOfKeys + 1) / 2); // do NOT copy
			// the median in case max_keys is odd
			// numOfKeys = (short) ((numOfKeys - 1) / 2 + 1); // draw 2 pictures
			// - for max_keys == odd and max_keys == even
			if (keyToInsert < _lastKey()) {
				_put(new Record<>(keyToInsert, justSplit.getPageId().getId()));
				// insert it and move last key to the sibling
				sibling._put(_lastPair());
				--numOfKeys;
			} else {
				sibling._put(new Record<>(keyToInsert, justSplit.getPageId()
					.getId()));
			}
			Record<Integer, Integer> _lastPair = _lastPair();
			writeShort(NUM_KEYS_OFFSET, --numOfKeys); // discard _lastPair
			buf.setPageDirty(this.getPageId().getId());
			setGreaterOrEqual(_lastPair.getValue());
			return new Record<Integer, Node>(_lastPair.getKey(), sibling);
		}

		Record<Integer, Node> insertInternal(Transaction tr, Node justSplit,
				Record<Integer, Node> insert) throws InterruptedException {
			final Node newNode = insert.getValue();
			Integer _keyOfAnchor = _keyWithValue(justSplit);
			if (_keyOfAnchor != null) {
				_put(_keyOfAnchor, newNode.getPageId().getId());
			} else {
				// _keyOfAnchor == null - anchor used to be for keys greater or
				// equal to lastKey
				setGreaterOrEqual(newNode.getPageId().getId());
			}
			if (overflow()) {// split
				return split(tr, new Record<>(insert.getKey(), justSplit));
			}
			_put(insert.getKey(), justSplit.getPageId().getId());
			return null;
		}

		@Override
		Collection<Node> print(Transaction tr, DBLock lock) throws IOException,
				InterruptedException {
			System.out.print(getPageId().getId() + "::");
			Collection<BPlusDisk<V>.Node> values = new ArrayList<>();
			for (short i = HEADER_SIZE, j = 0; j < numOfKeys; i += record_size, ++j) {
				int key = readInt(i);
				int val = readInt(i + key_size);
				values.add(newNodeFromDiskOrBuffer(tr, lock, val));
				System.out.print(key + ";" + val + ",");
			}
			System.out.print(greaterOrEqual() + "\t");
			values.add(newNodeFromDiskOrBuffer(tr, lock, greaterOrEqual()));
			return values;
		}
	}

	@SuppressWarnings("synthetic-access")
	final class LeafNode extends Node {

		Record<Integer, Node> insertInLeaf(Transaction tr,
				Record<Integer, Integer> rec) throws InterruptedException {
			if (overflow()) return split(tr, rec);
			_put(rec.getKey(), rec.getValue());
			return null;
		}

		@Deprecated
		public Map<Integer, Integer> records() {
			Map<Integer, Integer> m = new TreeMap<>();
			for (short i = HEADER_SIZE, j = 0; j < numOfKeys; i += record_size, ++j) {
				m.put(readInt(i), readInt(i + key_size));
			}
			return m;
		}

		/**
		 * Used in {@link #split(Record)} and in creating the tree for the first
		 * time (see {@link BPlusDisk#BPlusDisk(IndexDiskFile, short, short)}).
		 * In the latter case the transaction must be null.
		 */
		LeafNode(Transaction tr) throws InterruptedException {
			super(tr, true);
		}

		LeafNode(int id) throws InterruptedException, IOException {
			super(id);
		}

		@Override
		InternalNode parent(Transaction tr, DBLock lock, InternalNode root1)
				throws IOException, InterruptedException {
			final Integer id = getPageId().getId();
			if (root1._keyWithValue(this) != null
				|| id == root1.greaterOrEqual()) return root1;
			return parent(tr, lock, // the CCE will be thrown from this cast
				(InternalNode) root1._lookup(tr, lock, _firstPair().getKey()));
		}

		@Override
		LeafNode findLeaf(Transaction tr, DBLock lock, Integer k) {
			return this;
		}

		Record<Integer, Node>
				split(Transaction tr, Record<Integer, Integer> rec)
						throws InterruptedException {
			System.out.println("SPLIT LEAFNODE");
			LeafNode sibling = new LeafNode(tr);
			sibling.setGreaterOrEqual(greaterOrEqual());
			setGreaterOrEqual(sibling.getPageId().getId());
			// FIXME ALGORITHM
			// move median and up to sibling
			_copyTailAndRemoveIt(sibling, numOfKeys / 2); // ...
			// insert
			if (rec.getKey() < _lastKey()) {
				_put(rec.getKey(), rec.getValue());// insert in this
				sibling._put(_lastPair());
				--numOfKeys;
			} else {
				sibling._put(rec.getKey(), rec.getValue());
			}
			writeShort(NUM_KEYS_OFFSET, numOfKeys);
			buf.setPageDirty(this.getPageId().getId());
			return new Record<Integer, Node>(sibling._firstPair().getKey(),
				sibling);
		}

		@Override
		Collection<Node> print(Transaction tr, DBLock lock) {
			System.out.print(getPageId().getId() + ":"/* + numOfKeys */+ ":");
			for (short i = HEADER_SIZE, j = 0; j < numOfKeys; i += record_size, ++j) {
				int key = readInt(i);
				int val = readInt(i + key_size);
				System.out.print(key + ";" + val + ",");
			}
			System.out.print(greaterOrEqual() + "\t");
			return null;
		}
	}

	// =========================================================================
	// Helpers
	// =========================================================================
	private void insertInternal(Transaction tr, Node justSplit,
			Record<Integer, Node> insert) throws InterruptedException,
			IOException {
		if (root.getPageId().equals(justSplit.getPageId())) { // root must split
			// (leaf or not) // TODO use PageId<T> equals
			System.out.println("SPLIT ROOT");
			InternalNode newRoot = new InternalNode(tr);
			// FIXME ALGORITHM
			newRoot._put(insert.getKey(), justSplit.getPageId().getId());
			newRoot.setGreaterOrEqual(insert.getValue().getPageId().getId());
			root = newRoot; // FIXME write to file "root.txt"
			return;
		}
		// justSplit is not tree root so has a parent
		InternalNode parent = justSplit.parent(tr, DBLock.E,
			(InternalNode) root);
		// moreover root is not leaf so I cast it to InternalNode safely
		Record<Integer, Node> newInternalNode = parent.insertInternal(tr,
			justSplit, insert);
		if (newInternalNode != null)
			insertInternal(tr, parent, newInternalNode);
	}

	/**
	 * Inserts the record in the leaf - if it exists throws
	 * IllegalArgumentException. Must be called AFTER I lock the path to the
	 * leaf for writing.
	 */
	private <R extends Record<Integer, Integer>> void _insertInLeaf(
			Transaction tr, R rec, final LeafNode leafNode)
			throws IllegalArgumentException, InterruptedException, IOException {
		if (leafNode.records().containsKey(rec.getKey())) // FIXME records()!!!!
			throw new IllegalArgumentException("Key " + rec.getKey()
				+ " exists");
		Record<Integer, Node> insert = leafNode.insertInLeaf(tr, rec);
		if (insert != null) { // got a key back, so leafNode split
			insertInternal(tr, leafNode, insert); // all parents are locked
		}
	}
}
