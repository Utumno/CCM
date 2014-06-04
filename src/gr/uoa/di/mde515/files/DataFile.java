package gr.uoa.di.mde515.files;

import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.index.PageId;
import gr.uoa.di.mde515.index.Record;
import gr.uoa.di.mde515.locks.DBLock;

import java.io.IOException;
import java.util.List;

// TODO add type params for PageId<T>
public abstract class DataFile<K extends Comparable<K>, V> {

	public static <L extends Comparable<L>, M> DataFile<L, M> init(
			String filename, short recordSize) throws IOException,
			InterruptedException {
		return new HeapFile<>(filename, recordSize);
	}

	public abstract <T> PageId<T> insert(Transaction tr, Record<K, V> rec)
			throws IOException, InterruptedException;

	public abstract <T> void delete(Transaction tr, PageId<T> p, K key)
			throws IOException, InterruptedException;

	public abstract <T> V get(Transaction tr, PageId<T> p, K key)
			throws IOException, InterruptedException;

	public abstract void close() throws IOException;

	public abstract void flush(List<PageId<Integer>> pageIds)
			throws IOException;

	public abstract void abort(List<PageId<Integer>> pageIds)
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
