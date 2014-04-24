package gr.uoa.di.mde515.index;

import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.locks.Lock;
import gr.uoa.di.mde515.locks.LockManager;
import gr.uoa.di.mde515.locks.LockManager.Request;
import gr.uoa.di.mde515.trees.BPlusJava;
import gr.uoa.di.mde515.trees.BPlusJava.Node;

import java.util.SortedMap;
import java.util.TreeMap;

public class Index<K extends Comparable<K>, V> {

	public static final class KeyExistsException extends Exception {

		private static final long serialVersionUID = 720930361671317055L;

		public KeyExistsException(String string) {
			super("Key " + string + " exists");
		}
	}

	private BPlusJava<K, V> bplus;
	private LockManager<Node<K, V>> lm;

	public void lookupLocked(Transaction tr, K key, Lock el)
			throws KeyExistsException {
		SortedMap<K, V> sm = new TreeMap<>();
		lock(tr, key, el, sm);
		V v = sm.get(key);
		if (v != null) throw new KeyExistsException(key + "");
	}

	private void lock(Transaction tr, K key, Lock el, SortedMap<K, V> sm) {
		PageId<Node<K, V>> indexPage = bplus.getRootPageId();
		// TODO lock the root
		while (indexPage != null) {
			indexPage = bplus.getNextPageId(indexPage, key, sm);
			Request<Node<K, V>> request = new LockManager.Request<>(indexPage,
				tr, el);
			// TODO request lock for this page id and block till granted //
		}
	}
}
