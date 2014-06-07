package gr.uoa.di.mde515.index;

import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.locks.DBLock;
import gr.uoa.di.mde515.trees.BPlusJava;

import java.io.IOException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class IndexJava<K extends Comparable<K>, T> implements Index<K, T> {

	private final BPlusJava<K, T> bplus = new BPlusJava<>();

	@Override
	public T lookupLocked(Transaction tr, K key, DBLock el) throws IOException,
			InterruptedException {
		SortedMap<K, T> sm = new TreeMap<>();
		System.out.println(Thread.currentThread().getName());
		lockPath(tr, key, el, sm);
		return sm.get(key);
	}

	private void lockPath(Transaction tr, K key, DBLock el, SortedMap<K, T> sm) {
		throw new UnsupportedOperationException("Not implemented"); // TODO
		// PageId<Node<K, T>> indexPage = bplus.getRootPageId();
		// while (indexPage != null) {
		// tr.lock(indexPage, el);
		// indexPage = bplus.getNextPageId(indexPage, key, sm);
		// }
	}

	public void print() {
		bplus.print();
	}

	@Override
	public void flush(List<Integer> list) throws IOException {
		throw new UnsupportedOperationException("Not implemented"); // TODO
	}

	@Override
	public void insert(Transaction tr, Record<K, T> rec) throws IOException,
			InterruptedException {
		throw new UnsupportedOperationException("Not implemented"); // TODO
	}

	@Override
	public void delete(Transaction tr, K key) throws IOException,
			InterruptedException {
		throw new UnsupportedOperationException("Not implemented"); // TODO
	}

	@Override
	public void abort(List<Integer> list) throws IOException {
		throw new UnsupportedOperationException("Not implemented"); // TODO
	}
}
