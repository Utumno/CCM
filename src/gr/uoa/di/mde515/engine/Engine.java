package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.engine.CCM.TransactionRequiredException;
import gr.uoa.di.mde515.index.DataFile;
import gr.uoa.di.mde515.index.Index.KeyExistsException;
import gr.uoa.di.mde515.index.Record;

import java.util.concurrent.ExecutionException;

/**
 * Represents the DB external interface. It is a monofilestic engine but can be
 * extended to handle more files. Should be an interface implemented by enums
 * (for singleton property) but with the addition of a static factory (TODO:
 * java 8 ?)
 */
public abstract class Engine<K extends Comparable<K>, V> {

	public static final class TransactionFailedException extends Exception {

		public TransactionFailedException(ExecutionException e) {
			super("Transaction operation failed", e);
		}
	}

	private static final Engine instance = new EngineImpl();

	public static Engine newInstance() {
		return instance;
	}

	// =========================================================================
	// Abstract methods
	// =========================================================================
	public abstract Transaction beginTransaction();

	public abstract void endTransaction(Transaction tr);

	public abstract Record<K, V> insert(Transaction tr, Record<K, V> record)
			throws TransactionRequiredException, KeyExistsException,
			TransactionFailedException;
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
}

final class EngineImpl<K extends Comparable<K>, V> extends Engine<K, V> {

	private final CCM ccm;
	private final DataFile<K, V> dataFile = DataFile.init("");

	EngineImpl() {
		this.ccm = CCMImpl.instance();
	}

	@Override
	public Transaction beginTransaction() {
		return ccm.beginTransaction();
	}

	@Override
	public void endTransaction(Transaction tr) {
		ccm.endTransaction(tr);
	}

	@Override
	public Record<K, V> insert(Transaction tr, Record<K, V> record)
			throws TransactionRequiredException, KeyExistsException,
			TransactionFailedException {
		try {
			return ccm.insert(tr, record, dataFile);
		} catch (ExecutionException e) {
			throw new TransactionFailedException(e);
		}
	}
}
