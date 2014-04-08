package gr.uoa.di.mde515.db;

import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

public class BPlus<K extends Comparable<K>, V> {

	private Node<K, V> root = new LeafNode<>();
	private static final int N = 4;
	private static final int MAX_ITEMS = 2 * N + 1; // TODO

	static abstract class Node<K extends Comparable<K>, V> {

		// TODO list of parents (?)
		/** Return the Leaf that should contain key k starting from this Node */
		abstract LeafNode<K, V> findLeaf(K k);

		/** Split this Node and return the key pointing to new node (this.next) */
		abstract SortedMap<K, Node<K, V>> split();

		abstract InternalNode<K, V> parent(Node<K, V> root);

		boolean isLeaf() {
			return LeafNode.class.isAssignableFrom(this.getClass());
		}

		boolean overflow() {
			return items() == MAX_ITEMS;
		}

		abstract int items();
	}

	static class InternalNode<K extends Comparable<K>, V> extends Node<K, V> {

		/**
		 * A Sorted Map mapping Key key to Node node (Internal or Leaf) with
		 * node.lastKey strictly smaller than key. Define intNode.lastKey :=
		 * children.lastKey() and likewise for firstKey.
		 */
		SortedMap<K, Node<K, V>> children = new TreeMap<>();
		/** Node with keys strictly greater or equal to this.firstKey */
		Node<K, V> greaterOrEqual;

		private Node<K, V> _lookup(final K k) {
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

		K _keyOfValue(Node<K, V> anchor) {
			for (Entry<K, Node<K, V>> e : children.entrySet())
				if (e.getValue() == anchor) return e.getKey();
			if (greaterOrEqual != anchor)
				throw new RuntimeException("Node " + anchor
					+ " is not child of " + this);
			return null; // No key for this node
		}

		@Override
		InternalNode<K, V> parent(Node<K, V> root) {
			if (this == root) // eq ?
				return null;
			InternalNode<K, V> parent = (InternalNode<K, V>) root;
			if (parent.children.values().contains(this)) return parent;
			return parent(parent._lookup(this.children.lastKey()));
		}

		@Override
		LeafNode<K, V> findLeaf(final K k) {
			return _lookup(k).findLeaf(k);
		}

		@Override
		SortedMap<K, Node<K, V>> split() {
			final InternalNode<K, V> sibling = new InternalNode<>();
			sibling.greaterOrEqual = greaterOrEqual;
			final K median = new ArrayList<>(children.keySet()).get(N);
			greaterOrEqual = children.get(median);
			sibling.children = new TreeMap<>(children.tailMap(median));
			sibling.children.remove(median);
			this.children = new TreeMap<>(children.headMap(median));
			SortedMap<K, Node<K, V>> treeMap = new TreeMap<>();
			treeMap.put(median, sibling);
			return treeMap;
		}

		SortedMap<K, Node<K, V>> insertInternal(Node<K, V> anchorNode,
				SortedMap<K, Node<K, V>> insert) {
			final K keyToInsert = insert.firstKey();
			final Node<K, V> newNode = insert.get(keyToInsert);
			K _keyOfAnchor = _keyOfValue(anchorNode);
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
	}

	static class LeafNode<K extends Comparable<K>, V> extends Node<K, V> {

		SortedMap<K, V> records = new TreeMap<>();
		Node<K, V> next;

		@Override
		InternalNode<K, V> parent(Node<K, V> root) {
			if (this == root) return null;
			return (InternalNode<K, V>) root;
		}

		@Override
		LeafNode<K, V> findLeaf(K k) {
			return this;
		}

		SortedMap<K, Node<K, V>> insertInLeaf(Record<K, V> rec) {
			records.put(rec.getKey(), rec.getValue());
			return overflow() ? split() : null;
		}

		@Override
		SortedMap<K, Node<K, V>> split() {
			LeafNode<K, V> sibling = new LeafNode<>();
			sibling.next = next;
			next = sibling;
			final K median = new ArrayList<>(records.keySet()).get(N);
			// move median and up to sibling
			sibling.records = new TreeMap<>(records.tailMap(median));
			// keep the rest
			this.records = new TreeMap<>(records.headMap(median));
			// below in python would be return median, sibling -:(
			SortedMap<K, Node<K, V>> treeMap = new TreeMap<>();
			treeMap.put(median, sibling);
			return treeMap;
		}

		@Override
		int items() {
			return records.size();
		}
	}

	public <R extends Record<K, V>> void insert(R rec) {
		final K key = rec.getKey();
		final LeafNode<K, V> leafNode = findLeaf(key); // find where the key
		// must go
		if (leafNode.records.containsKey(key))
			throw new IllegalArgumentException("Key exists");
		SortedMap<K, Node<K, V>> insert = leafNode.insertInLeaf(rec);
		if (insert != null) { // got a key back, so leafNode split
			insertInternal(leafNode, insert);
		}
	}

	private void insertInternal(Node<K, V> anchor,
			SortedMap<K, Node<K, V>> insert) {
		if (root == anchor) { // root must split (leaf or not)
			InternalNode<K, V> newRoot = new InternalNode<>();
			newRoot.children.put(insert.firstKey(), anchor);
			newRoot.greaterOrEqual = insert.get(insert.firstKey());
			root = newRoot;
			return;
		}
		InternalNode<K, V> parent = anchor.parent(root); // root is not leaf
															// here
		SortedMap<K, Node<K, V>> insertInternal = parent.insertInternal(parent,
			insert);
		if (insertInternal != null) insertInternal(parent, insertInternal);
	}

	private LeafNode<K, V> findLeaf(final K key) {
		Node<K, V> current = root;
		while (!current.isLeaf()) {
			current = current.findLeaf(key);
		}
		return (LeafNode<K, V>) current;
	}
}
