package gr.uoa.di.mde515.db;

public interface IBPlus<K extends Comparable<K>, V> {

	void print();

	<R extends Record<K, V>> void insert(R rec);
}
