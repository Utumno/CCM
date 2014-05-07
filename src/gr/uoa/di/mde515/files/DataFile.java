package gr.uoa.di.mde515.files;

import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.index.PageId;
import gr.uoa.di.mde515.index.Record;
import gr.uoa.di.mde515.locks.DBLock;

import java.io.IOException;
import java.util.List;

public abstract class DataFile<K extends Comparable<K>, V> {

	public static <L extends Comparable<L>, M> DataFile<L, M> init(
			String filename) throws IOException, InterruptedException {
		return new HeapFile<>(filename);
	}

	public abstract void insert(Transaction tr, Record<K, V> rec)
			throws IOException, InterruptedException;

	public abstract void close() throws IOException;

	public abstract void flush(List<PageId<Integer>> pageIds)
			throws IOException;

	/**
	 * Attempts to lock the header page on behalf of transaction {@code tr},
	 * blocks till it manages to do so.
	 *
	 * @param tr
	 *            the transaction that attempts to lock
	 * @param e
	 *            the type of lock requested
	 */
	public abstract void lockHeader(Transaction tr, DBLock e);
}
