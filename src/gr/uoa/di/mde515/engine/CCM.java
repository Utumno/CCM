package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.db.Record;

import java.io.File; // FIXME
import java.util.List;

public interface CCM<T> {

	Transaction b_xaction();

	void e_xaction();

	Record<T> insert(Transaction tr, Record<T> record);

	Record<T> delete(T key);

	Record<T> lookup(T key);

	Record<T> update(T key);

	List<Record<T>> range(T key1, T key2);

	boolean waitTransaction(long t);

	void abort();

	void commit();

	File bulk_load(File fileOfRecords);

	File bulk_delete(File fileOfKeys);
}

class CCMImpl<T> implements CCM<T> {

	List<Transaction>
;

	// thread pool

	@Override
	public Transaction b_xaction() {
		synchronized (transactions) {
			final Transaction tr = new Transaction();
			transactions.add(tr);
			return tr;
		}
	}

	@Override
	public void e_xaction() {
		// TODO Auto-generated method stub
	}

	@Override
	public Record<T> insert(Transaction tr, Record<T> record) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Record<T> delete(T key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Record<T> lookup(T key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Record<T> update(T key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Record<T>> range(T key1, T key2) {
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


