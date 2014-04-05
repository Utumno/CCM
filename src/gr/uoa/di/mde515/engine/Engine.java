package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.db.Record;

public abstract class Engine<T> { // MUST BE AN INTERFACE - EntityManager


	Engine() {}

	public abstract Transaction b_xaction();

	public abstract void e_xaction(Transaction tr);

	public abstract Record<T> insert(Transaction tr, Record<T> record);

	//
	// Record<T> delete(T key);
	//
	// Record<T> lookup(T key);
	//
	// Record<T> update(T key);
	//
	// List<Record<T>> range(T key1, T key2);
	//
	// boolean waitTransaction(long t);
	//
	// void abort();
	//
	// void commit();
	//
	// File bulk_load(File fileOfRecords);
	//
	// File bulk_delete(File fileOfKeys);
	public static Engine<?> newInstance() {
		return new EngineImpl();
	}
}

class EngineImpl<T> extends Engine<T> {

	CCM<T> ccm;

	@Override
	public Transaction b_xaction() {
		return ccm.b_xaction();
	}
	@Override
	public void e_xaction(Transaction tr) {
		// TODO Auto-generated method stub
	}

	@Override
	public Record<T> insert(Transaction tr, Record<T> record) {
		return ccm.insert(tr, record);
	}
}
