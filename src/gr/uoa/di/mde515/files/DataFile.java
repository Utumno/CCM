package gr.uoa.di.mde515.index;

import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.engine.buffer.HF;
import gr.uoa.di.mde515.locks.DBLock;

import java.io.IOException;

public abstract class DataFile<K extends Comparable<K>, V> {

	public static <L extends Comparable<L>, M> DataFile<L, M> init(
			String filename) throws IOException, InterruptedException {
		return new HF<L, M>(filename);
	}

	public abstract void insert(Transaction tr, Record<K, V> rec)
			throws IOException, InterruptedException;

	public abstract void close() throws IOException;

	public abstract void lockHeader(Transaction tr, DBLock e);
}
