package gr.uoa.di.mde515.db;

import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

public class BPlus<K extends Comparable<K>, V> {

	private Node<K, V> root = new LeafNode<>();
	private static final int N = 4;
	private static final int MAX_ITEMS = 2 * N + 1;

	static abstract class Node<K extends Comparable<K>, V> {

		// TODO list of parents (?)
		Node<K, V> next;

		/** Return the Leaf that should contain key k starting from this Node */
		abstract LeafNode<K, V> findLeaf(K k);

		/** Split this Node and return the key pointing to new node (this.next) */
		abstract K split();

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

		/** key --> Node (Internal or leaf) with keys strictly smaller than key */
		SortedMap<K, Node<K, V>> children;
		Node<K, V> greaterOrEqual;

		@Override
		InternalNode<K, V> parent(Node<K, V> root) {
			if (this == root) // eq ?
				return null;
			InternalNode<K, V> parent = (InternalNode<K, V>) root;
			if (parent.children.values().contains(this)) return parent;
			K k = this.children.lastKey();
			if (k.compareTo(parent.children.lastKey()) >= 0)
				return parent(greaterOrEqual);
			SortedMap<K, Node<K, V>> tailMap = parent.children.tailMap(k);
			// tailMap contains at least parent.children.lastKey()
			final K firstKey = tailMap.firstKey();
			if (firstKey.compareTo(k) == 0) { // k == tm0
				final K exists = new ArrayList<>(tailMap.keySet()).get(1);
				return parent(tailMap.get(exists));
			}
			return parent(parent.children.get(firstKey));
		}

		@Override
		LeafNode<K, V> findLeaf(final K k) {
			if (k.compareTo(children.lastKey()) >= 0)
				return greaterOrEqual.findLeaf(k);
			// tailMap contains at least children.lastKey()
			SortedMap<K, Node<K, V>> tailMap = this.children.tailMap(k);
			final K firstKey = tailMap.firstKey(); // [tm0, tm1,...] where
			// k <= tmi < tmj for 0 <= i < j < tailMap.keySet().size()
			if (firstKey.compareTo(k) == 0) { // k == tm0
				final K exists = new ArrayList<>(tailMap.keySet()).get(1);
				return tailMap.get(exists).findLeaf(k);
			}
			return children.get(firstKey).findLeaf(k);
		}

		@Override
		K split() {
			InternalNode<K, V> sibling = new InternalNode<>();
			// sibling.next = next;
			// next = sibling;
			final K median = new ArrayList<>(children.keySet()).get(N);
			sibling.children = new TreeMap<>(children.tailMap(median));
			this.records = new TreeMap<>(records.headMap(median));
			return median;
			throw new RuntimeException("Not impl");
		}

		InternalNode<K, V> insertInternal(Node<K, V> anchorNode, K key,
				Node<K, V> newNode) {
			if (children.lastKey().compareTo(key) >= 0) {
				return ((Object) greaterOrEqual).insertInternal(key, newNode);
			}
			return null;
		}

		@Override
		int items() {
			return children.size();
		}
	}

	static class LeafNode<K extends Comparable<K>, V> extends Node<K, V> {

		SortedMap<K, V> records;

		@Override
		InternalNode<K, V> parent(Node<K, V> root) {
			if (this == root) return null;
			return (InternalNode<K, V>) root;
		}

		@Override
		LeafNode<K, V> findLeaf(K k) {
			return this;
		}

		K insertInLeaf(Record<K, V> rec) {
			records.put(rec.getKey(), rec.getValue());
			if (records.size() < MAX_ITEMS) {
				return null; // all ok
			}
			return split(); // k must point to the new Leaf node
		}

		@Override
		K split() {
			LeafNode<K, V> sibling = new LeafNode<>();
			sibling.next = next;
			next = sibling;
			final K median = new ArrayList<>(records.keySet()).get(N);
			sibling.records = new TreeMap<>(records.tailMap(median));
			this.records = new TreeMap<>(records.headMap(median));
			return median;
		}

		@Override
		int items() {
			return records.size();
		}
	}

	<R extends Record<K, V>> void insert(R rec) {
		final K key = rec.getKey();
		final LeafNode<K, V> leafNode = findLeaf(key); // find where the key
		// must go
		if (leafNode.records.containsKey(key))
			throw new IllegalArgumentException("Key exists");
		K insert = leafNode.insertInLeaf(rec);
		if (insert != null) { // got a key back, so leafNode split
			insert = insertInternal(leafNode, insert, leafNode.next);
		}
		return;
	}

	private K insertInternal(Node<K, V> anchor, K insert, Node<K, V> next) {
		if (root == anchor) { // root must split
			InternalNode<K, V> newRoot = new InternalNode<>();
			newRoot.children = new TreeMap<>();
			newRoot.children.put(insert, anchor);
			newRoot.greaterOrEqual = next;
			root = newRoot;
			return null;
		}
		InternalNode<K, V> parent = anchor.parent(root);
		final SortedMap<K, Node<K, V>> parentChildr = parent.children;
		parentChildr.put(insert, next);
		if (!parent.overflow()) return null;
		K split = parent.split();
		((InternalNode<K, V>) root).insertInternal(parent, split,
			parent.greaterOrEqual); // greaterOrEqual should be next
	}

	private LeafNode<K, V> findLeaf(final K key) {
		Node<K, V> current = root;
		while (!current.isLeaf()) {
			current = current.findLeaf(key);
		}
		return (LeafNode<K, V>) current;
	}
}
