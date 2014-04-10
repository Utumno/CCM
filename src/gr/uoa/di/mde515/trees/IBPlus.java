package gr.uoa.di.mde515.trees;

import gr.uoa.di.mde515.index.Record;

public interface IBPlus<K extends Comparable<K>, V> {

	void print();

	<R extends Record<K, V>> void insert(R rec);
}
