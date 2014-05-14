package gr.uoa.di.mde515.trees;

import gr.uoa.di.mde515.engine.Engine;
import gr.uoa.di.mde515.engine.buffer.BufferManager;
import gr.uoa.di.mde515.engine.buffer.Page;
import gr.uoa.di.mde515.files.IndexDiskFile;
import gr.uoa.di.mde515.index.PageId;
import gr.uoa.di.mde515.index.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
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

	// FIXME flush on engine shutdown
	//
	// function object - to be used in generifying
	// private final T diskRec;
	private static final BufferManager<Integer> buf = BufferManager
		.getInstance();
	private final IndexDiskFile file;
	// fields
	private Node root;
	private AtomicInteger nodeId = new AtomicInteger(0);
	// sizes of the key and value to be used in the tree
	private final short key_size;
	private final short value_size;
	private final short record_size;

	static final class Root { // serialization proxy really

		private static final Path ROOT_PATH = FileSystems.getDefault().getPath(
			"root.txt");
		private static final Path NODES_PATH = FileSystems.getDefault()
			.getPath("nodes.txt");

		static int rootFromFile() throws IOException {
			byte[] readAllBytes = Files.readAllBytes(ROOT_PATH);
			return Integer.valueOf(new String(readAllBytes,
				StandardCharsets.ISO_8859_1));
		}

		static void rootToFile(int newRoot) throws IOException {
			Files.write(ROOT_PATH, ByteBuffer.allocate(4).putInt(newRoot)
				.array());
		}

		static int nodesFromFile() throws IOException {
			byte[] readAllBytes = Files.readAllBytes(NODES_PATH);
			return Integer.valueOf(new String(readAllBytes,
				StandardCharsets.ISO_8859_1));
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
			int rootFromFile = Root.rootFromFile();
			Page<Integer> allocFrame = buf.allocFrame(rootFromFile, file);
			boolean leaf = ((BPlusDisk<V>.Node) allocFrame)
				.isLeafReadFromPage();
			root = (leaf) ? new LeafNode(rootFromFile) : new InternalNode(
				rootFromFile);
			// FIXME permanent alloc (and something else I forgot ...)
			nodeId.set(Root.nodesFromFile());
		} else { // FILE EMPTY - CREATE THE ROOT
			System.out.println(file + ": Creating...");
			root = new LeafNode();
			buf.setPageDirty(-1);
			buf.flushPage(-1, file); // TODO wild flush
		}
	}

	public <R extends Record<Integer, Integer>> void insert(R rec)
			throws IOException, InterruptedException {
		final LeafNode leafNode = root.findLeaf(rec.getKey());
		_insertInLeaf(rec, leafNode);
	}

	/** Return a page id for the root node */
	public PageId<Integer> getRootPageId() {
		return root.getPageId();
	}

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
	public PageId<Node> getNextPageId(PageId<Node> grantedPage, Integer key,
			SortedMap<Integer, Integer> sm) throws IOException,
			InterruptedException {
		Node node = grantedPage.getId();
		if (node.isLeaf()) {
			sm.putAll(((LeafNode) node).records());
			return null; // locked the path to the key
		}
		InternalNode in = (InternalNode) node;
		final Node nextNode = in._lookup(key);
		return new PageId<>(nextNode);
	}

	public <R extends Record<Integer, Integer>> PageId<Node> getLeaf(
			PageId<Node> grantedPage, R rec) throws IOException,
			InterruptedException {
		Node node = grantedPage.getId();
		final Integer key = rec.getKey();
		if (node.isLeaf()) {
			_insertInLeaf(rec, (LeafNode) node); // all parents are
			// locked
			return null; // granted
		}
		InternalNode in = (InternalNode) node;
		final Node nextNode = in._lookup(key);
		return new PageId<>(nextNode);
	}

	public void print() throws IOException, InterruptedException {
		List<Node> items = new ArrayList<>();
		items.add(root);
		while (!items.isEmpty()) {
			List<Node> children = new ArrayList<>();
			for (Node node : items) {
				final Collection<Node> print = node.print();
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
		private static final short NUM_KEYS_OFFSET = 1;
		private final boolean isLeaf;
		private final short max_keys = (short) ((Engine.PAGE_SIZE - HEADER_SIZE - key_size) / record_size);
		{
			System.out.println("MAX KEYS " + max_keys);
		}

		private Node(int id) throws IOException, InterruptedException {
			super(buf.allocFrame(id, file));
			isLeaf = readByte(LEAF_OFFSET) == 1;
			numOfKeys = readShort(NUM_KEYS_OFFSET);
		}

		/**
		 * Allocates a Node IN MEMORY
		 */
		private Node(boolean leaf) throws InterruptedException {
			super(buf.allocFrameForNewPage(nodeId.decrementAndGet()));
			isLeaf = leaf;
			writeByte(LEAF_OFFSET, (byte) ((leaf) ? 1 : 0));
			numOfKeys = 0;
			writeShort(NUM_KEYS_OFFSET, (short) 0);
		}

		Node newMemoryNode(boolean leaf) throws InterruptedException {
			if (leaf) return new LeafNode();
			return new InternalNode();
		}

		Node newNodeFromDiskOrBuffer(Transaction tr, DBLock lock, int pageID)
				throws IOException, InterruptedException {
			Page<Integer> allocFrame = buf.allocFrame(id, file);
			boolean leaf = allocFrame.readByte(LEAF_OFFSET) == 1;
			if (leaf) return new LeafNode(id);
			return new InternalNode(id);
		}

		/**
		 * Return the Leaf that should contain key k starting from this Node
		 *
		 * @throws InterruptedException
		 * @throws IOException
		 */
		abstract LeafNode findLeaf(Integer k) throws IOException,
				InterruptedException;

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
		abstract InternalNode parent(InternalNode candidateParent)
				throws IOException, InterruptedException;

		boolean overflow() {
			return numOfKeys == max_keys; // FIXME ............ Test
		}

		boolean isLeafReadFromPage() {
			return readByte(LEAF_OFFSET) == 1;
		}

		int items() {
			return numOfKeys;
		}

		abstract Collection<Node> print() throws IOException,
				InterruptedException;

		@Override
		public String toString() {
			return "@" + getPageId().getId();
		}

		// private Node greaterOrEqual() {
		// int offset = Engine.PAGE_SIZE - key_size;
		// final Integer keyFromBytes = diskRec.getKeyFromBytes(readBytes(
		// offset, key_size));
		// return new Node(keyFromBytes);
		// }
		int greaterOrEqual() {
			return readInt(Engine.PAGE_SIZE - key_size);
		}

		void setGreaterOrEqual(int goe) {
			writeInt(Engine.PAGE_SIZE - key_size, goe);
		}

		void _put(Record<Integer, Integer> rec) {
			_put(rec.getKey(), rec.getValue());
		}
		// byte[] readBytes(int position, short howMany) {
		// byte[] result = new byte[howMany];
		// data.get(result, position, howMany);
		// return result;
		// }
		boolean isLeaf() {
			return isLeaf;
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
		}

		/** Tries to insert key and value and stops at the middle of the node */
		Record<Integer, Integer> _newMedian(Record<Integer, Node> rec) {
			int i, j;
			int key = rec.getKey();
			int value = rec.getValue().getPageId().getId();
			for (i = HEADER_SIZE, j = 0; j < (numOfKeys + 1) / 2; i += record_size, ++j) {
				int tmpKey = readInt(i);
				int tmpValue = readInt(i + key_size);
				if (key < tmpKey) {
					writeInt(i, key);
					writeInt(i + key_size, value);
					key = tmpKey;
					value = tmpValue;
				}
			}
			return new Record<>(key, value); // return the median node
		}

		void _copyTail(Node sibling, int from) {
			for (int j = from, i = HEADER_SIZE + j * record_size; j < numOfKeys; i += record_size, ++j) {
				sibling._put(readInt(i), readInt(i + key_size));
			}
		}

		int _medianKey() {
			return readInt(HEADER_SIZE + ((numOfKeys + 1) / 2) * record_size);
		}

		Record<Integer, Integer> _median() {
			int key = readInt(HEADER_SIZE + ((numOfKeys + 1) / 2) * record_size);
			int value = readInt(HEADER_SIZE + ((numOfKeys + 1) / 2)
				* record_size + key_size);
			return new Record<>(key, value);
		}
	}

	@SuppressWarnings("synthetic-access")
	class InternalNode extends Node {

		public InternalNode() throws InterruptedException {
			super(false);
		}

		public InternalNode(int id) throws IOException, InterruptedException {
			super(id);
		}

		Node _lookup(final Integer k) throws IOException, InterruptedException {
			if (k.compareTo(_lastKey()) >= 0)
				return newNodeFromDisk(greaterOrEqual());
			// tailMap contains at least children.lastKey()
			for (short i = HEADER_SIZE; i < numOfKeys; i += record_size) {
				int readInt = readInt(i);
				if (k.compareTo(readInt) >= 0) {
					if (k.compareTo(readInt) > 0)
						return newNodeFromDisk(readInt(i + key_size));
					return newNodeFromDisk(readInt(i + 2 * key_size
						+ value_size));
				}
			}
			throw new RuntimeException("key " + k + " not found");
		}

		/**
		 * Wrappers around _lookup(K k) - look a node up means look its lastKey
		 * up
		 *
		 * @throws InterruptedException
		 * @throws IOException
		 */
		private Node _lookup(InternalNode internalNode) throws IOException,
				InterruptedException {
			return _lookup(internalNode._lastKey());
		}

		private Integer _keyWithValue(Node anchor) {
			final int id = anchor.getPageId().getId();
			for (short i = HEADER_SIZE; i < numOfKeys; i += record_size) {
				int readInt = readInt(i);
				if (id == readInt) {
					return readInt(i - key_size);
				}
			}
			// if (greaterOrEqual.equals(anchor))
			// throw new RuntimeException("Node " + anchor
			// + " is not child of " + this);
			return null; // No key for this node
		}

		@Override
		InternalNode parent(InternalNode root1) throws IOException,
				InterruptedException {
			if (greaterOrEqual() == root1.greaterOrEqual()) return root1;
			final int id = getPageId().getId();
			for (short i = HEADER_SIZE; i < numOfKeys; i += record_size) {
				if (id == root1.readInt(i)) return root1;
			}
			return parent((InternalNode) root1._lookup(this)); // the CCE
		}

		@Override
		LeafNode findLeaf(final Integer k) throws IOException,
				InterruptedException {
			return _lookup(k).findLeaf(k);
		}

		Record<Integer, Node> split(Record<Integer, Node> insert)
				throws InterruptedException {
			final int keyToInsert = insert.getKey();
			final BPlusDisk<V>.Node justSplitvalueToInsert = insert.getValue();
			int medianKey = _medianKey();
			Record<Integer, Integer> median;
			if (keyToInsert < medianKey) {
				median = _newMedian(new Record<>(keyToInsert,
					justSplitvalueToInsert));// insert
				// it and recalculate the median
			} else {
				median = _median();
			}
			final InternalNode sibling = new InternalNode(); // FIXME LOCK
																// !!!?????
			sibling.setGreaterOrEqual(greaterOrEqual());
			setGreaterOrEqual(median.getValue());
			_copyTail(sibling, (numOfKeys + 1) / 2);
			numOfKeys = (short) (numOfKeys / 2);
			return new Record<Integer, Node>(median.getKey(), sibling);
		}

		Record<Integer, Node> insertInternal(Node justSplit,
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
				return split(new Record<>(insert.getKey(), justSplit));
			}
			_put(insert.getKey(), justSplit.getPageId().getId());
			return null;
		}

		@Override
		Collection<Node> print() throws IOException, InterruptedException {
			System.out.print(getPageId().getId() + "::");
			Collection<BPlusDisk<V>.Node> values = new ArrayList<>();
			for (int i = HEADER_SIZE; i < numOfKeys; i++) {
				int key = readInt(i);
				int val = readInt(i + key_size);
				values.add(newNodeFromDiskOrBuffer(val));
				System.out.print(key + ";" + val + ",");
			}
			System.out.print(greaterOrEqual() + "\t");
			values.add(newNodeFromDiskOrBuffer(greaterOrEqual()));
			return values;
		}
	}

	class LeafNode extends Node {

		Record<Integer, Node> insertInLeaf(Record<Integer, Integer> rec)
				throws InterruptedException {
			if (overflow()) return split(rec);
			_put(rec.getKey(), rec.getValue());
			return null;
		}

		public Map<Integer, Integer> records() {
			Map<Integer, Integer> m = new TreeMap<>();
			for (short i = HEADER_SIZE; i < numOfKeys; i += record_size) {
				m.put(readInt(i), readInt(i + key_size));
			}
			return m;
		}

		public LeafNode() throws InterruptedException {
			super(true);
		}

		public LeafNode(int id) throws InterruptedException, IOException {
			super(id);
		}

		@Override
		InternalNode parent(InternalNode root1) throws IOException,
				InterruptedException {
			if (root1._keyWithValue(this) != null
				|| greaterOrEqual() == root1.greaterOrEqual()) return root1;
			return parent((InternalNode) root1
				._lookup(this.getPageId().getId())); // the CCE
		}

		@Override
		LeafNode findLeaf(Integer k) {
			return this;
		}

		Record<Integer, Node> split(Record<Integer, Integer> rec)
				throws InterruptedException {
			LeafNode sibling = new LeafNode(); // FIXME LOCK !!!?????
			sibling.setGreaterOrEqual(greaterOrEqual());
			setGreaterOrEqual(sibling.getPageId().getId());
			final int _medianKeyPreSplit = _medianKey();
			// move median and up to sibling
			_copyTail(sibling, (numOfKeys + 1) / 2 - 1);
			// keep the rest
			numOfKeys /= 2;
			// insert
			if (rec.getKey() < _medianKeyPreSplit) {
				_put(rec.getKey(), rec.getValue());// insert in this
				sibling._put(_lastPair());
				--numOfKeys;
			} else {
				sibling._put(rec.getKey(), rec.getValue());
			}
			return new Record<Integer, Node>(sibling._firstPair().getKey(),
				sibling);
		}

		@Override
		Collection<Node> print() {
			System.out.print(getPageId().getId() + "::");
			for (int i = HEADER_SIZE; i < numOfKeys; i++) {
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
	private void insertInternal(Node justSplit, Record<Integer, Node> insert)
			throws InterruptedException, IOException {
		if (root.getPageId().equals(justSplit.getPageId())) { // root must split
																// (leaf or not)
			InternalNode newRoot = new InternalNode();
			newRoot._put(insert.getKey(), justSplit.getPageId().getId());
			newRoot.setGreaterOrEqual(insert.getValue().getPageId().getId());
			root = newRoot; // FIXME write to file
			return;
		}
		// justSplit is not tree root so has a parent
		InternalNode parent = justSplit.parent((InternalNode) root);
		// moreover root is not leaf so I cast it to InternalNode safely
		Record<Integer, Node> newInternalNode = parent.insertInternal(
			justSplit, insert);
		if (newInternalNode != null) insertInternal(parent, newInternalNode);
	}

	private <R extends Record<Integer, Integer>> void _insertInLeaf(R rec,
			final LeafNode leafNode) throws IllegalArgumentException,
			InterruptedException, IOException {
		if (leafNode.records().containsKey(rec.getKey())) // FIXME records()!!!!
			throw new IllegalArgumentException("Key exists");
		Record<Integer, Node> insert = leafNode.insertInLeaf(rec);
		if (insert != null) { // got a key back, so leafNode split
			insertInternal(leafNode, insert); // all parents are locked
		}
	}
}
