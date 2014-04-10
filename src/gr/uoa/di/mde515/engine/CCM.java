package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.index.Record;

import java.io.File; // FIXME
import java.util.List;

public interface CCM<K extends Comparable<K>, V> {

	Transaction beginTransaction();

	void endTransaction();

	Record<K, V> insert(Transaction tr, Record<K, V> record);

	Record<K, V> delete(K key);

	Record<K, V> lookup(K key);

	Record<K, V> update(K key);

	List<Record<K, V>> range(K key1, K key2);

	boolean waitTransaction(long t);

	void abort();

	void commit();

	File bulk_load(File fileOfRecords);

	File bulk_delete(File fileOfKeys);
}

class CCMImpl<K extends Comparable<K>, V> implements CCM<K, V> {

	List<Transaction> transactions;

	// thread pool
	@Override
	public Transaction beginTransaction() {
		synchronized (transactions) {
			final Transaction tr = new Transaction();
			transactions.add(tr);
			return tr;
		}
	}

	@Override
	public void endTransaction() {
		// TODO Auto-generated method stub
	}

	@Override
	public Record<K, V> insert(Transaction tr, Record<K, V> record) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Record<K, V> delete(K key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Record<K, V> lookup(K key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Record<K, V> update(K key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Record<K, V>> range(K key1, K key2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean waitTransaction(long t) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void abort() {
		// TODO Auto-generated method stub
	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub
	}

	@Override
	public File bulk_load(File fileOfRecords) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public File bulk_delete(File fileOfKeys) {
		// TODO Auto-generated method stub
		return null;
	}
}
