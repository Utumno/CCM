package gr.uoa.di.mde515.index;

import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.engine.buffer.Page;
import gr.uoa.di.mde515.locks.Lock;
import gr.uoa.di.mde515.locks.LockManager;
import gr.uoa.di.mde515.locks.LockManager.Request;
import gr.uoa.di.mde515.trees.BPlusJava;
import gr.uoa.di.mde515.trees.BPlusJava.Node;

import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;

public class Index<K extends Comparable<K>, V> {

	private BPlusJava<K, V> bplus;
	private LockManager<Node<K, V>> lm;

	public Page lookupLocked(Transaction tr, K key, Lock el) {
		SortedMap<K, V> sm = new TreeMap<>();
		lock(tr, key, el, sm);
		V v = sm.get(key);
		if (v != null) return (Page) v;// v should be a page id for a page in
		// the records file
		K prevkey = null, nextkey = null;
		try {
			prevkey = sm.headMap(key).lastKey();
		} catch (NoSuchElementException e) {} // key would be first in this leaf
		try {
			nextkey = sm.tailMap(key).firstKey();
		} catch (NoSuchElementException e) {} // key would be last in this leaf
		// FIXME !!! what do I return ?
	}

	private void lock(Transaction tr, K key, Lock el, SortedMap<K, V> sm) {
		PageId<Node<K, V>> indexPage = bplus.getRootPageId();
		while (indexPage != null) {
			indexPage = bplus.getNextPageId(indexPage, key, sm);
			Request<Node<K, V>> request = new LockManager.Request<>(indexPage,
				tr, el);
			// TODO request lock for this page id and block till granted //
		}
	}
}
