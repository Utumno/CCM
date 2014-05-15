package gr.uoa.di.mde515.trees;

import gr.uoa.di.mde515.index.Record;

// WIP - use the implementations directly !
// problem is that the disc implementation is hard to generify
public interface BPlusTree<K extends Comparable<K>, V> {

	void print();

	<R extends Record<K, V>> void insert(R rec);
}
