package gr.uoa.di.mde515.index;

import gr.uoa.di.mde515.engine.HeapFile;
import gr.uoa.di.mde515.engine.Transaction;

public abstract class DataFile<K extends Comparable<K>, V> {

	public static <L extends Comparable<L>, M> DataFile<L, M> init(
			String filename) {
		return new HeapFile<L, M>(filename);
	}

	public abstract void insert(Transaction tr, Record<K, V> rec);
}
