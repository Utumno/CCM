package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.engine.CCM.TransactionRequiredException;
import gr.uoa.di.mde515.files.DataFile;
import gr.uoa.di.mde515.index.Index;
import gr.uoa.di.mde515.index.Index.KeyExistsException;
import gr.uoa.di.mde515.index.PageId;
import gr.uoa.di.mde515.index.Record;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Represents the DB external interface. It is a monofilestic engine but can be
 * extended to handle more files, by making the methods instead of the class
 * generic. This though leads to some complications given that the files have
 * also generic parameters. Should be an interface implemented by enums (for
 * singleton property) but with the addition of a static factory (TODO: java 8
 * ?). <br/>
 * All methods that require a Transaction will throw a
 * {@code NullPointerException} if the supplied transaction is {@code null} and
 * a {@link TransactionRequiredException} if the transaction supplied is not
 * valid.
 *
 * @param <K>
 *            the key type of the records in the one and only one file. Must
 *            extend {@link Comparable}
 * @param <V>
 *            the type of the records value - that is all the attributes except
 *            the key. No restrictions here.
 */
public abstract class Engine<K extends Comparable<K>, V> {

	public static final class TransactionFailedException extends Exception {

		private static final long serialVersionUID = -4298165326203675694L;

		public TransactionFailedException(ExecutionException e) {
			super("Transaction operation failed", e);
		}
	}

	private static final Engine instance = new EngineImpl();

	public static Engine newInstance() {
		return instance;
	}

	public abstract void shutEngine() throws InterruptedException, IOException;

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
	public abstract void commit(Transaction tr);

	//
	// File bulk_load(File fileOfRecords);
	//
	// File bulk_delete(File fileOfKeys);
	public abstract void print();
}

final class EngineImpl<K extends Comparable<K>, V, T> extends Engine<K, V> {

	private final CCM ccm;
	private final DataFile<K, V> dataFile;
	final Index<K, PageId<T>> index = new Index<>();

	EngineImpl() {
		this.ccm = CCMImpl.instance();
		try {
			dataFile = DataFile.init("temp.db");
		} catch (IOException | InterruptedException e) {
			throw new RuntimeException("Can't open db file", e);
		}
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
			return ccm.insert(tr, record, dataFile, index);
		} catch (ExecutionException e) {
			throw new TransactionFailedException(e);
		}
	}

	@Override
	public void commit(Transaction tr) {
		tr.flush();
	}

	@Override
	public void shutEngine() throws InterruptedException, IOException {
		ccm.shutdown();
		dataFile.close();
	}

	@Override
	public void print() {
		index.print();
	}
}
