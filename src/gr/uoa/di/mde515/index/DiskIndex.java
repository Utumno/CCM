package gr.uoa.di.mde515.index;

import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.engine.buffer.Serializer;
import gr.uoa.di.mde515.files.IndexDiskFile;
import gr.uoa.di.mde515.locks.DBLock;
import gr.uoa.di.mde515.trees.BPlusDisk;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class DiskIndex<K extends Comparable<K>, T> implements Index<K, T> {

	private final BPlusDisk<K, T> bplus;

	public DiskIndex(IndexDiskFile file, Serializer<K> serKey,
			Serializer<T> serVal) throws IOException, InterruptedException {
		bplus = new BPlusDisk<>(file, serKey, serVal);
	}

	/**
	 * Locks the path from the root to the leaf where a key is to be inserted on
	 * behalf of a given transaction - WIP
	 *
	 * @return the value for {@code key} or null if {@code key} is not found
	 * @throws InterruptedException
	 *             from the buffer manager
	 * @throws IOException
	 *             from the disk file
	 */
	@Override
	public T lookupLocked(Transaction tr, K key, DBLock el) throws IOException,
			InterruptedException {
		Map<K, T> sm = new HashMap<>();
		bplus.lockPath(tr, key, el, sm);
		return sm.get(key);
	}

	public void print(Transaction tr, DBLock lock) throws IOException,
			InterruptedException {
		bplus.print(tr, lock);
	}

	@Override
	public void flush(List<Integer> list) throws IOException {
		bplus.flush(list);
	}

	@Override
	public void insert(Transaction tr, Record<K, T> rec) throws IOException,
			InterruptedException {
		bplus.insert(tr, rec);
	}

	@Override
	public void abort(List<Integer> list) throws IOException {
		bplus.abort(list);
	}

	@Override
	public void delete(Transaction tr, K key) throws IOException,
			InterruptedException {
		bplus.delete(tr, key);
	}
}
