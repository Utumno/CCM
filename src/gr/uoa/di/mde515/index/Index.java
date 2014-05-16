package gr.uoa.di.mde515.index;

import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.locks.DBLock;
import gr.uoa.di.mde515.locks.LockManager;
import gr.uoa.di.mde515.trees.BPlusJava;
import gr.uoa.di.mde515.trees.BPlusJava.Node;

import java.util.SortedMap;
import java.util.TreeMap;

public class Index<K extends Comparable<K>, V> { // V --> rename to T

	private final BPlusJava<K, V> bplus = new BPlusJava<>();
	private final LockManager lm = LockManager.getInstance();

	/**
	 * Locks the path from the root to the leaf where a key is to be inserted on
	 * behalf of a given transaction - WIP
	 *
	 * @throws KeyExistsException
	 *             if the key exists
	 */
	public void lookupLocked(Transaction tr, K key, DBLock el)
			throws KeyExistsException {
		SortedMap<K, V> sm = new TreeMap<>();
		lockPath(tr, key, el, sm);
		V v = sm.get(key);
		if (v != null) throw new KeyExistsException(key + "");
	}

	private void lockPath(Transaction tr, K key, DBLock el, SortedMap<K, V> sm) {
		PageId<Node<K, V>> indexPage = bplus.getRootPageId();
		while (indexPage != null) {
			lm.requestLock(new LockManager.Request(indexPage, tr, el));
			indexPage = bplus.getNextPageId(indexPage, key, sm);
		}
	}

	public void print() {
		bplus.print();
	}
}
