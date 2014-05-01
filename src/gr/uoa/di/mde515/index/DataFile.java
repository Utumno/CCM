package gr.uoa.di.mde515.index;

import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.engine.buffer.HF;

public abstract class DataFile<K extends Comparable<K>, V> {

	public static <L extends Comparable<L>, M> DataFile<L, M> init(
			String filename) {
		return new HF<L, M>(filename);
	}

	public abstract void insert(Transaction tr, Record<K, V> rec);
}
