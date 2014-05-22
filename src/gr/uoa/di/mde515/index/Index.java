package gr.uoa.di.mde515.index;

import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.locks.DBLock;

import java.io.IOException;
import java.util.List;

/**
 * @param <K>
 *            the key type - the primary key in the data file
 * @param <T>
 *            the type of the pageId - int in this project
 */
public interface Index<K extends Comparable<K>, T> {

	/**
	 * Locks the path from the root to the leaf where a key is to be inserted on
	 * behalf of a given transaction - WIP
	 *
	 * @throws KeyExistsException
	 *             if the key exists
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public abstract T lookupLocked(Transaction tr, K key, DBLock el)
			throws IOException, InterruptedException;

	public abstract void flush(List<PageId<Integer>> list) throws IOException;

	public abstract void insert(Transaction tr, Record<K, T> rec)
			throws IOException, InterruptedException;

	public abstract void delete(Transaction tr, K key) throws IOException,
			InterruptedException;

	public abstract void abort(List<PageId<Integer>> list) throws IOException;
}
