package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.db.Record;

public abstract class Engine<K extends Comparable<K>, V> { // MUST BE AN
															// INTERFACE -
															// EntityManager


	Engine() {}

	public abstract Transaction b_xaction();

	public abstract void e_xaction(Transaction tr);

	public abstract Record<K, V> insert(Transaction tr, Record<K, V> record);

	//
	// Record<K,V> delete(T key);
	//
	// Record<K,V> lookup(T key);
	//
	// Record<K,V> update(T key);
	//
	// List<Record<K,V>> range(T key1, T key2);
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
	public static Engine<?, ?> newInstance() {
		return new EngineImpl();
	}
}

class EngineImpl<K extends Comparable<K>, V> extends Engine<K, V> {

	CCM<K, V> ccm;

	@Override
	public Transaction b_xaction() {
		return ccm.b_xaction();
	}
	@Override
	public void e_xaction(Transaction tr) {
		// TODO Auto-generated method stub
	}

	@Override
	public Record<K, V> insert(Transaction tr, Record<K, V> record) {
		return ccm.insert(tr, record);
	}
}
