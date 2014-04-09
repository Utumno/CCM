package gr.uoa.di.mde515.db;

public class Index<K extends Comparable<K>, V> {

	private BPlusJava<K, V> bplus;

	public void insert(Record<K, V> rec) {
		bplus.insert(rec);
	}
}
