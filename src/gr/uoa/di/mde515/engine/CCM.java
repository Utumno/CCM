package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.index.Record;

import java.io.File; // FIXME
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public interface CCM<K extends Comparable<K>, V> {

	final class TransactionRequiredException extends Exception {

		private static final long serialVersionUID = 1451632646226806301L;

		public TransactionRequiredException() {
			super("This method must be called while in an active transaction.");
		}
	}

	Transaction beginTransaction();

	void endTransaction(Transaction tr);

	Record<K, V> insert(Transaction tr, Record<K, V> record)
			throws TransactionRequiredException;

	Record<K, V> delete(Transaction tr, K key);

	Record<K, V> lookup(Transaction tr, K key);

	Record<K, V> update(Transaction tr, K key);

	List<Record<K, V>> range(Transaction tr, K key1, K key2);

	boolean waitTransaction(Transaction tr, long t);

	void abort(Transaction tr);

	void commit(Transaction tr);

	File bulkLoad(Transaction tr, File fileOfRecords);

	File bulkDelete(Transaction tr, File fileOfKeys);
}

class CCMImpl<K extends Comparable<K>, V> implements CCM<K, V> {

	final List<Transaction> transactions = Collections
		.synchronizedList(new ArrayList<Transaction>()); // ...

	// TODO thread pool
	@Override
	public Transaction beginTransaction() {
		final Transaction tr = new Transaction();
		transactions.add(tr);
		return tr;
	}

	@Override
	public void endTransaction(Transaction tr) {
		throw new UnsupportedOperationException("Not supported yet."); // TODO
	}

	@Override
	public Record<K, V> insert(Transaction tr, Record<K, V> record) {
		throw new UnsupportedOperationException("Not supported yet."); // TODO
	}

	@Override
	public Record<K, V> delete(Transaction tr, K key) {
		throw new UnsupportedOperationException("Not supported yet."); // TODO
	}

	@Override
	public Record<K, V> lookup(Transaction tr, K key) {
		throw new UnsupportedOperationException("Not supported yet."); // TODO
	}

	@Override
	public Record<K, V> update(Transaction tr, K key) {
		throw new UnsupportedOperationException("Not supported yet."); // TODO
	}

	@Override
	public List<Record<K, V>> range(Transaction tr, K key1, K key2) {
		throw new UnsupportedOperationException("Not supported yet."); // TODO
	}

	@Override
	public boolean waitTransaction(Transaction tr, long t) {
		throw new UnsupportedOperationException("Not supported yet."); // TODO
	}

	@Override
	public void abort(Transaction tr) {
		throw new UnsupportedOperationException("Not supported yet."); // TODO
	}

	@Override
	public void commit(Transaction tr) {
		throw new UnsupportedOperationException("Not supported yet."); // TODO
	}

	@Override
	public File bulkLoad(Transaction tr, File fileOfRecords) {
		throw new UnsupportedOperationException("Not supported yet."); // TODO
	}

	@Override
	public File bulkDelete(Transaction tr, File fileOfKeys) {
		throw new UnsupportedOperationException("Not supported yet."); // TODO
	}
}
