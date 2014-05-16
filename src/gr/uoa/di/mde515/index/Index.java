package gr.uoa.di.mde515.index;

import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.locks.DBLock;
import gr.uoa.di.mde515.locks.LockManager;
import gr.uoa.di.mde515.trees.BPlusJava;
import gr.uoa.di.mde515.trees.BPlusJava.Node;

import java.io.IOException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @param <K>
 *            the key type - the primary key in the data file
 * @param <T>
 *            the type of the pageId - int in this project
 */
public class Index<K extends Comparable<K>, T> {

	private final BPlusJava<K, T> bplus = new BPlusJava<>();
	private final LockManager lm = LockManager.getInstance();

	/**
	 * Locks the path from the root to the leaf where a key is to be inserted on
	 * behalf of a given transaction - WIP
	 *
	 * @throws KeyExistsException
	 *             if the key exists
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public T lookupLocked(Transaction tr, K key, DBLock el) throws IOException,
			InterruptedException {
		SortedMap<K, T> sm = new TreeMap<>();
		lockPath(tr, key, el, sm);
		return sm.get(key);
	}

	private void lockPath(Transaction tr, K key, DBLock el, SortedMap<K, T> sm) {
		PageId<Node<K, T>> indexPage = bplus.getRootPageId();
		while (indexPage != null) {
			lm.requestLock(new LockManager.Request(indexPage, tr, el));
			indexPage = bplus.getNextPageId(indexPage, key, sm);
		}
	}

	public void print() {
		bplus.print();
	}

	public void flush(List<PageId<Integer>> list) throws IOException {
		throw new UnsupportedOperationException("Not implemented"); // TODO
	}

	public void print(Transaction tr, DBLock lock) throws IOException,
			InterruptedException {
		throw new UnsupportedOperationException("Not implemented"); // TODO
	}

	public void insert(Transaction tr, Record<K, Integer> rec)
			throws IOException, InterruptedException {
		throw new UnsupportedOperationException("Not implemented"); // TODO
	}
}
