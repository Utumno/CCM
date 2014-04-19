package gr.uoa.di.mde515.index;

import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.locks.Lock;
import gr.uoa.di.mde515.locks.LockManager;
import gr.uoa.di.mde515.locks.LockManager.Request;
import gr.uoa.di.mde515.trees.BPlusJava;
import gr.uoa.di.mde515.trees.BPlusJava.Node;

public class Index<K extends Comparable<K>, V> {

	private BPlusJava<K, V> bplus;
	private LockManager<Node<K, V>> lm;

	public void insert(Record<K, V> rec) {
		Lock el = Lock.E;
		PageId<Node<K, V>> indexPage = bplus.insertIterative(rec);
		while (indexPage != null) {
			indexPage = bplus.insertIterative(indexPage, rec);
			Request<Node<K, V>> request = new LockManager.Request<>(indexPage,
				new Transaction(), el);
			// request lock for this page id and block till granted //
		}
		V value = rec.getValue(); // this should now insert into the file
		// V should be a page id for a page in the file
	}
}
