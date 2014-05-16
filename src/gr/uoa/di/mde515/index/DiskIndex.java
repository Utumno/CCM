package gr.uoa.di.mde515.index;

import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.files.IndexDiskFile;
import gr.uoa.di.mde515.locks.DBLock;
import gr.uoa.di.mde515.locks.LockManager;
import gr.uoa.di.mde515.trees.BPlusDisk;

import java.io.IOException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class DiskIndex<K extends Comparable<K>, V> extends Index<K, V> {

	private final BPlusDisk bplus;
	private final LockManager lm = LockManager.getInstance();

	public DiskIndex(IndexDiskFile file, short key_size, short value_size)
			throws IOException, InterruptedException {
		bplus = new BPlusDisk(file, key_size, value_size);
	}

	/**
	 * Locks the path from the root to the leaf where a key is to be inserted on
	 * behalf of a given transaction - WIP
	 *
	 * @throws KeyExistsException
	 *             if the key exists
	 * @throws InterruptedException
	 * @throws IOException
	 */
	@Override
	public void lookupLocked(Transaction tr, K key, DBLock el)
			throws KeyExistsException, IOException, InterruptedException {
		SortedMap<K, V> sm = new TreeMap<>();
		lockPath(tr, key, el, sm);
		V v = sm.get(key);
		if (v != null) throw new KeyExistsException(key + "");
	}

	private void lockPath(Transaction tr, K key, DBLock el, SortedMap<K, V> sm)
			throws IOException, InterruptedException {
		throw new UnsupportedOperationException("Not implemented"); // TODO
	}

	@Override
	public void print(Transaction tr, DBLock lock) throws IOException,
			InterruptedException {
		bplus.print(tr, lock);
	}

	@Override
	public void flush(List<PageId<Integer>> list) throws IOException {
		bplus.flush(list);
	}

	@Override
	public void insert(Transaction tr, Record<Integer, Integer> rec)
			throws IOException, InterruptedException {
		bplus.insert(tr, rec);
	}
}
