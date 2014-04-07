package gr.uoa.di.mde515.db;

import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

public class BPlus<K extends Comparable<K>, V> {

	private Node<K, V> root = new LeafNode<>();
	private static final int N = 4;
	private static final int MAX_ITEMS = 2 * N + 1;

	static abstract class Node<K extends Comparable<K>, V> {

		private Node<K, V> next;

		Node<K, V> next() {
			return getNext();
		}

		abstract Node<K, V> insert(Record<K, V> rec);

		abstract Node<K, V> findLeaf(K k);

		abstract K split();

		boolean isLeaf() {
			return false;
		}

		Node<K, V> getNext() {
			return next;
		}

		void setNext(Node<K, V> next) {
			this.next = next;
		}
	}

	static class InternalNode<K extends Comparable<K>, V> extends Node<K, V> {

		SortedMap<K, Node<K, V>> children;
		private Node<K, V> greaterOrEqual;

		@Override
		Node<K, V> findLeaf(K k) {
			SortedMap<K, Node<K, V>> tailMap = this.children.tailMap(k);
			if (tailMap.isEmpty()) {
				return greaterOrEqual.findLeaf(k);
			}
			final K firstKey = tailMap.firstKey();
			if (firstKey.compareTo(k) == 0) {
				return greaterOrEqual.findLeaf(k);
			}
			return children.get(firstKey).findLeaf(k);
		}

		@Override
		Node<K, V> insert(Record<K, V> rec) {
			throw new RuntimeException("Not impl");
		}
	}

	static class LeafNode<K extends Comparable<K>, V> extends Node<K, V> {

		SortedMap<K, V> records;

		@Override
		boolean isLeaf() {
			return true;
		}

		@Override
		Node<K, V> findLeaf(K k) {
			return this;
		}

		@Override
		Node<K, V> insert(Record<K, V> rec) {
			if (records.size() + 1 == MAX_ITEMS) split();
			throw new RuntimeException("Not impl");
		}

		@Override
		K split() {
			LeafNode<K, V> sibling = new LeafNode<>();
			LeafNode<K, V> next2 = (LeafNode<K, V>) this.next();
			sibling.setNext(this.next());
			this.setNext(sibling);
			final K median = new ArrayList<>(records.keySet()).get(N);
			sibling.records = new TreeMap<>(records.tailMap(median));
			this.records = new TreeMap<>(records.headMap(median));
			return median;
		}
	}

	<R extends Record<K, V>> R insert(R rec) {
		final K key = rec.getKey();
		final LeafNode<K, V> leafNode = findLeaf(key);
		if (leafNode.records.containsKey(key))
			throw new IllegalArgumentException("Key exists");
		final Node<K, V> insert = leafNode.insert(rec);
		return rec;
	}

	private LeafNode<K, V> findLeaf(final K key) {
		Node<K, V> current = root;
		while (!current.isLeaf()) {
			current = current.findLeaf(key);
		}
		return (LeafNode<K, V>) current;
	}
}
