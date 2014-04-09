package gr.uoa.di.mde515.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

public class BPlusJava<K extends Comparable<K>, V> implements IBPlus<K, V> {

	private Node<K, V> root = new LeafNode<>();
	private static final int N = 1;
	private static final int MAX_ITEMS = 2 * N + 1; // TODO
	private int _leafs = 1; // to be used in printing
	private int _levels = 1;

	// =========================================================================
	// API
	// =========================================================================
	@Override
	public <R extends Record<K, V>> void insert(R rec) {
		final K key = rec.getKey();
		final LeafNode<K, V> leafNode = findLeaf(key); // find where the key
		// must go
		if (leafNode.records.containsKey(key))
			throw new IllegalArgumentException("Key exists");
		Record<K, Node<K, V>> insert = leafNode.insertInLeaf(rec);
		if (insert != null) { // got a key back, so leafNode split
			++_leafs;
			insertInternal(leafNode, insert);
		}
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
	static abstract class Node<K extends Comparable<K>, V> {

		// TODO list of parents (?)
		/** Return the Leaf that should contain key k starting from this Node */
		abstract LeafNode<K, V> findLeaf(K k);

		/** Split this Node - return key and new node to insert in parent node */
		abstract Record<K, Node<K, V>> split();

		abstract InternalNode<K, V> parent(Node<K, V> fromTreeRoot);

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
		/** Node with keys strictly greater or equal to this.firstKey */
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

		private K _keyWithValue(Node<K, V> anchor) {
			for (Entry<K, Node<K, V>> e : children.entrySet())
				if (e.getValue() == anchor) return e.getKey();
			if (greaterOrEqual != anchor)
				throw new RuntimeException("Node " + anchor
					+ " is not child of " + this);
			return null; // No key for this node
		}

		@Override
		InternalNode<K, V> parent(Node<K, V> root) {
			System.out.println("this" + this + " parent" + root);
			if (this == root) return null; // eq ?
			InternalNode<K, V> parent = (InternalNode<K, V>) root;
			if (parent.children.values().contains(this)
				|| this == parent.greaterOrEqual) return parent;
			return parent(parent._lookup(this.children.lastKey()));
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

		Record<K, Node<K, V>> insertInternal(Node<K, V> anchorNode,
				Record<K, Node<K, V>> insert) {
			final K keyToInsert = insert.getKey();
			final Node<K, V> newNode = insert.getValue();
			K _keyOfAnchor = _keyWithValue(anchorNode);
			if (_keyOfAnchor != null) {
				children.put(_keyOfAnchor, newNode);
				children.put(keyToInsert, anchorNode); // all keys in anchor
				// node are smaller than the key to insert
			} else {
				// _keyOfAnchor == null - anchor used to be for keys greater or
				// equal to lastKey
				children.put(keyToInsert, anchorNode);
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
			records.put(rec.getKey(), rec.getValue());
			return overflow() ? split() : null;
		}

		@Override
		InternalNode<K, V> parent(Node<K, V> root) {
			if (root instanceof LeafNode) return null; // tree root is leaf
			InternalNode<K, V> parent = (InternalNode<K, V>) root;
			if (parent.children.values().contains(this)
				|| this == parent.greaterOrEqual) return parent;
			return parent(parent._lookup(this.records.lastKey()));
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
	private void
			insertInternal(Node<K, V> anchor, Record<K, Node<K, V>> insert) {
		if (root == anchor) { // root must split (leaf or not)
			++_levels;
			InternalNode<K, V> newRoot = new InternalNode<>();
			newRoot.children.put(insert.getKey(), anchor);
			newRoot.greaterOrEqual = insert.getValue();
			root = newRoot;
			return;
		}
		InternalNode<K, V> parent = anchor.parent(root); // root is not leaf
		Record<K, Node<K, V>> newInternalNode = parent.insertInternal(anchor,
			insert);
		if (newInternalNode != null) insertInternal(parent, newInternalNode);
	}

	private LeafNode<K, V> findLeaf(final K key) {
		return root.findLeaf(key);
	}
}
