package gr.uoa.di.mde515.trees;

import gr.uoa.di.mde515.index.PageId;
import gr.uoa.di.mde515.index.Record;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * B plus tree. When a node that accepts an EVEN number of items splits the
 * middle node is moved to the RIGHT sibling. For instance (for max number of
 * items equal to 4 (N=2, 2*N + 1 = 5)):
 *
 * 12345 -> 12 345
 *
 * @param <K>
 *            the type of the key of the records to be stored in the leaf *and*
 *            internal nodes. Must extend Comparable.
 * @param <V>
 *            the type of the value of the records to be stored in the leaf
 *            nodes (non key attributes)
 */
public class BPlusJava<K extends Comparable<K>, V> implements BPlusTree<K, V> {

	private Node<K, V> root = new LeafNode<>();
	private static final int N = 1;
	private static final int MAX_ITEMS = 2 * N + 1; // TODO, not only odd
	private int _leafs = 1; // to be used in printing
	private int _levels = 1; // to be used in printing

	// =========================================================================
	// API
	// =========================================================================
	@Override
	/** Insert a key-value pair into a leaf node. */
	public <R extends Record<K, V>> void insert(R rec) {
		final K key = rec.getKey();
		final LeafNode<K, V> leafNode = root.findLeaf(key); // find where the
		// key must go
		_insertInLeaf(rec, key, leafNode);
	}

	/** Return a page id for the root node */
	public PageId<Node<K, V>> getRootPageId() {
		return new PageId<>(root);
	}

	/**
	 * Returns the next page id of the index we need to lock in our search for
	 * the given key. When the grantedPage is a leaf node this function returns
	 * null to signal we locked all the path to the key while at the same time
	 * populating the sm output parameter with the map of keys and page ids in
	 * the leaf node.
	 */
	public PageId<Node<K, V>> getNextPageId(PageId<Node<K, V>> grantedPage,
			K key, SortedMap<K, V> sm) {
		Node<K, V> node = grantedPage.getId();
		if (node instanceof LeafNode) {
			sm.putAll(((LeafNode) node).records);
			return null; // locked the path to the key
		}
		InternalNode<K, V> in = (InternalNode<K, V>) node;
		final Node<K, V> nextNode = in._lookup(key);
		return new PageId<>(nextNode);
	}

	public <R extends Record<K, V>> PageId<Node<K, V>> getLeaf(
			PageId<Node<K, V>> grantedPage, R rec) {
		Node<K, V> node = grantedPage.getId();
		final K key = rec.getKey();
		if (node instanceof LeafNode) {
			_insertInLeaf(rec, key, (LeafNode<K, V>) node); // all parents are
			// locked
			return null; // granted
		}
		InternalNode<K, V> in = (InternalNode<K, V>) node;
		final Node<K, V> nextNode = in._lookup(key);
		return new PageId<>(nextNode);
	}

	@Override
	public void print() {
		List<Node<K, V>> items = new ArrayList<>();
		items.add(root);
		while (!items.isEmpty()) {
			List<Node<K, V>> children = new ArrayList<>();
			for (Node<K, V> node : items) {
				final Collection<Node<K, V>> print = node.print(_leafs);
				if (print != null) children.addAll(print);
			}
			System.out.println();
			items = children;
		}
	}

	// =========================================================================
	// Nodes
	// =========================================================================
	public static abstract class Node<K extends Comparable<K>, V> {

		// TODO list of parents (?)
		/** Return the Leaf that should contain key k starting from this Node */
		abstract LeafNode<K, V> findLeaf(K k);

		/** Split this Node - return key and new node to insert in parent node */
		abstract Record<K, Node<K, V>> split();

		/**
		 * Do not call *on* tree root. For (non tree root) Node n,
		 * n.parent(tree_root) will return the parent of n. tree_root should be
		 * Internal unless the only node in the tree.
		 *
		 * @throws ClassCastException
		 *             if no parent found (tries to find child nodes of leafs)
		 */
		abstract InternalNode<K, V> parent(InternalNode<K, V> candidateParent);

		boolean overflow() {
			return items() == MAX_ITEMS;
		}

		abstract int items();

		abstract Collection<Node<K, V>> print(int leafs);

		@Override
		public String toString() {
			return "@" + hashCode();
		}
	}

	static class InternalNode<K extends Comparable<K>, V> extends Node<K, V> {

		/**
		 * A Sorted Map mapping Key k to Node n (Internal or Leaf) with
		 * n.lastKey strictly smaller than k. Define intNode.lastKey :=
		 * children.lastKey() and likewise for firstKey.
		 */
		SortedMap<K, Node<K, V>> children = new TreeMap<>();
		/** Node with keys strictly greater or equal to this.lastKey */
		Node<K, V> greaterOrEqual; // TODO rename to next and move to Node

		Node<K, V> _lookup(final K k) {
			if (k.compareTo(children.lastKey()) >= 0) return greaterOrEqual;
			// tailMap contains at least children.lastKey()
			SortedMap<K, Node<K, V>> tailMap = this.children.tailMap(k);
			final K firstKey = tailMap.firstKey(); // [tm0, tm1,...] where
			// k <= tmi < tmj for 0 <= i < j < tailMap.keySet().size()
			if (k.compareTo(firstKey) == 0) { // k == tm0
				final K exists = new ArrayList<>(tailMap.keySet()).get(1);
				return tailMap.get(exists);
			}
			return children.get(firstKey);
		}

		/**
		 * Wrappers around _lookup(K k) - look a node up means look its lastKey
		 * up
		 */
		Node<K, V> _lookup(LeafNode<K, V> root) {
			return _lookup(root.records.lastKey());
		}

		private Node<K, V> _lookup(InternalNode<K, V> internalNode) {
			return _lookup(internalNode.children.lastKey());
		}

		private K _keyWithValue(Node<K, V> anchor) {
			for (Entry<K, Node<K, V>> e : children.entrySet())
				if (e.getValue() == anchor) return e.getKey();
			if (greaterOrEqual != anchor)
				throw new RuntimeException("Node " + anchor
					+ " is not child of " + this);
			return null; // No key for this node
		}

		@Override
		InternalNode<K, V> parent(InternalNode<K, V> root) {
			if (root.children.values().contains(this)
				|| this == root.greaterOrEqual) return root;
			return parent((InternalNode<K, V>) root._lookup(this)); // the CCE
		}

		@Override
		LeafNode<K, V> findLeaf(final K k) {
			return _lookup(k).findLeaf(k);
		}

		@Override
		Record<K, Node<K, V>> split() {
			final InternalNode<K, V> sibling = new InternalNode<>();
			sibling.greaterOrEqual = greaterOrEqual;
			final K median = new ArrayList<>(children.keySet()).get(N);
			greaterOrEqual = children.get(median);
			sibling.children = new TreeMap<>(children.tailMap(median));
			sibling.children.remove(median);
			this.children = new TreeMap<>(children.headMap(median));
			return new Record<K, Node<K, V>>(median, sibling);
		}

		Record<K, Node<K, V>> insertInternal(Node<K, V> justSplit,
				Record<K, Node<K, V>> insert) {
			final K keyToInsert = insert.getKey();
			final Node<K, V> newNode = insert.getValue();
			K _keyOfAnchor = _keyWithValue(justSplit);
			if (_keyOfAnchor != null) {
				children.put(_keyOfAnchor, newNode);
				children.put(keyToInsert, justSplit); // all keys in anchor
				// node are smaller than the key to insert
			} else {
				// _keyOfAnchor == null - anchor used to be for keys greater or
				// equal to lastKey
				children.put(keyToInsert, justSplit);
				greaterOrEqual = newNode;
			}
			return overflow() ? split() : null;
		}

		@Override
		int items() {
			return children.size();
		}

		@Override
		Collection<Node<K, V>> print(int leafs) {
			System.out.print(this + "::");
			System.out.print(Arrays.toString(children.entrySet().toArray()));
			System.out.print(greaterOrEqual + "\t");
			final Collection<Node<K, V>> values = new ArrayList<>(
				children.values());
			values.add(greaterOrEqual);
			return values;
		}
	}

	static class LeafNode<K extends Comparable<K>, V> extends Node<K, V> {

		SortedMap<K, V> records = new TreeMap<>();
		Node<K, V> next;

		Record<K, Node<K, V>> insertInLeaf(Record<K, V> rec) {
			records.put(rec.getKey(), rec.getValue()); // Need 2 pages if this
			// overflows and implementing it with page buffers
			return overflow() ? split() : null;
		}

		@Override
		InternalNode<K, V> parent(InternalNode<K, V> root) {
			if (root.children.values().contains(this)
				|| this == root.greaterOrEqual) return root;
			return parent((InternalNode<K, V>) root._lookup(this)); // the CCE
		}

		@Override
		LeafNode<K, V> findLeaf(K k) {
			return this;
		}

		@Override
		Record<K, Node<K, V>> split() {
			LeafNode<K, V> sibling = new LeafNode<>();
			sibling.next = next;
			next = sibling;
			final K median = new ArrayList<>(records.keySet()).get(N);
			// move median and up to sibling
			sibling.records = new TreeMap<>(records.tailMap(median));
			// keep the rest
			this.records = new TreeMap<>(records.headMap(median));
			return new Record<K, Node<K, V>>(median, sibling);
		}

		@Override
		int items() {
			return records.size();
		}

		@Override
		Collection<Node<K, V>> print(int leafs) {
			System.out.print(this + "::");
			System.out.print(Arrays.toString(records.entrySet().toArray()));
			System.out.print(next + "\t");
			return null;
		}
	}

	// =========================================================================
	// Helpers
	// =========================================================================
	private void insertInternal(Node<K, V> justSplit,
			Record<K, Node<K, V>> insert) {
		if (root == justSplit) { // root must split (leaf or not)
			++_levels;
			InternalNode<K, V> newRoot = new InternalNode<>();
			newRoot.children.put(insert.getKey(), justSplit);
			newRoot.greaterOrEqual = insert.getValue();
			root = newRoot;
			return;
		}
		// justSplit is not tree root so has a parent
		InternalNode<K, V> parent = justSplit.parent((InternalNode<K, V>) root);
		// moreover root is not leaf so I cast it to InternalNode safely
		Record<K, Node<K, V>> newInternalNode = parent.insertInternal(
			justSplit, insert);
		if (newInternalNode != null) insertInternal(parent, newInternalNode);
	}

	private <R extends Record<K, V>> void _insertInLeaf(R rec, final K key,
			final LeafNode<K, V> leafNode) throws IllegalArgumentException {
		if (leafNode.records.containsKey(key))
			throw new IllegalArgumentException("Key exists");
		Record<K, Node<K, V>> insert = leafNode.insertInLeaf(rec);
		if (insert != null) { // got a key back, so leafNode split
			++_leafs;
			insertInternal(leafNode, insert); // all parents are locked
		}
	}
}
