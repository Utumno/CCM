package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.engine.CCM.TransactionRequiredException;
import gr.uoa.di.mde515.engine.buffer.Serializer;
import gr.uoa.di.mde515.files.DataFile;
import gr.uoa.di.mde515.files.IndexDiskFile;
import gr.uoa.di.mde515.index.DiskIndex;
import gr.uoa.di.mde515.index.Index;
import gr.uoa.di.mde515.index.IndexJava;
import gr.uoa.di.mde515.index.KeyExistsException;
import gr.uoa.di.mde515.index.Record;
import gr.uoa.di.mde515.locks.DBLock;

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
 *            extend {@link Comparable} - the current implementation expects
 *            Integers (there are casts that will blow if not) FIXME - generify
 * @param <V>
 *            the type of the records value - that is all the attributes except
 *            the key. No restrictions here.
 * @param <T>
 *            the type of the page IDs - the current implementation expects
 *            Integers (there are casts that will blow if not) FIXME - generify
 */
public abstract class Engine<K extends Comparable<K>, V, T> {

	public abstract void submit(TransactionalOperation to);

	public abstract class TransactionalOperation {

		final Transaction trans;

		public TransactionalOperation() {
			trans = beginTransaction();
		}

		public abstract void execute();

		public Record<K, V> insert(Record<K, V> record)
				throws TransactionRequiredException, KeyExistsException,
				TransactionFailedException {
			return Engine.this.insert(trans, record);
		}

		public void endTransaction() {
			throw new UnsupportedOperationException("Not implemented"); // TODO
		}
	}

	public static final class TransactionFailedException extends Exception {

		private static final long serialVersionUID = -4298165326203675694L;

		public TransactionFailedException(ExecutionException e) {
			super("Transaction operation failed", e);
		}
	}

	public static final short PAGE_SIZE = 72;
	private static volatile Engine<?, ?, ?> instance;
	private final static Object HACK = new Object(); // TODO fix this mess

	public static <K extends Comparable<K>, V, T> Engine<?, ?, ?> newInstance(
			Serializer<K, T> ser) {
		if (instance == null) synchronized (HACK) {
			if (instance == null) instance = new EngineImpl<K, V, T>(ser);
		}
		return instance;
	}

	public abstract void shutdown() throws InterruptedException, IOException;

	// =========================================================================
	// Abstract methods
	// =========================================================================
	public abstract Transaction beginTransaction();

	public abstract void endTransaction(Transaction tr);

	public abstract Record<K, V> insert(Transaction tr, Record<K, V> record)
			throws TransactionRequiredException, KeyExistsException,
			TransactionFailedException;

	public abstract void commit(Transaction tr) throws IOException;

	public abstract void waitTransaction(long time) throws InterruptedException;

	public abstract void abort(Transaction tr) throws IOException;

	public abstract void delete(Transaction tr, K key, DBLock el)
			throws KeyExistsException, IOException, InterruptedException,
			TransactionRequiredException, ExecutionException;

	public abstract Record<K, V> lookup(Transaction tr, K key, DBLock el)
			throws KeyExistsException, IOException, InterruptedException; // TODO
	// boolean lookup
	//
	// Record<K,V> update(T key);
	//
	// List<Record<K,V>> range(T key1, T key2);
	//
	// File bulk_load(File fileOfRecords);
	//
	// File bulk_delete(File fileOfKeys);
	// =========================================================================
	// Debug
	// =========================================================================

	/** ONLY FOR DEBUG */
	public abstract void deleteIndex(Transaction tr, K rec) throws IOException,
			InterruptedException;

	/** ONLY FOR DEBUG */
	public abstract void print();

	/** ONLY FOR DEBUG */
	public abstract void insertIndex(Transaction tr, Record<K, T> rec)
			throws IOException, InterruptedException;

	/** ONLY FOR DEBUG _ WILL LOCK THE WHOLE TREE */
	public abstract void print(Transaction tr, DBLock e) throws IOException,
			InterruptedException;
}

final class EngineImpl<K extends Comparable<K>, V, T> extends Engine<K, V, T> {

	private static final String DB_FILE = "temp.db";
	private static final String INDEX_FILE = "index.db";
	private static final short RECORD_SIZE = 8; // TODO bin
	private final CCM ccm;
	private final DataFile<K, V> dataFile;
	private final Index<K, T> index;

	EngineImpl(Serializer<K, T> ser) {
		this.ccm = CCMImpl.instance();
		String opening = DB_FILE;
		try {
			dataFile = DataFile.init(opening, RECORD_SIZE);
			opening = INDEX_FILE;
			index = new DiskIndex<>(new IndexDiskFile(opening), ser);
			System.out.println("ENGINE INITIALIZED");
		} catch (IOException | InterruptedException e) {
			throw new RuntimeException("Can't open " + opening + " file", e);
		}
	}

	@Override
	public void submit(Engine<K, V, T>.TransactionalOperation to) {
		ccm.submit(to);
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
	public void commit(Transaction tr) throws IOException {
		ccm.commit(tr, dataFile, index);
	}

	@Override
	public void waitTransaction(long time) throws InterruptedException {
		Thread.sleep(time);
	}

	@Override
	public void abort(Transaction tr) throws IOException {
		ccm.abort(tr, dataFile, index);
	}

	@Override
	public void shutdown() throws InterruptedException, IOException {
		ccm.shutdown();
		dataFile.close();
	}

	@Override
	public Record<K, V> lookup(Transaction tr, K key, DBLock el)
			throws KeyExistsException, IOException, InterruptedException {
		return ccm.lookup(tr, key, el, dataFile, index);
	}

	@Override
	public void delete(Transaction tr, K key, DBLock el) throws IOException,
			InterruptedException, TransactionRequiredException,
			ExecutionException {
		ccm.delete(tr, key, el, dataFile, index);
	}

	// =========================================================================
	// Debug
	// =========================================================================
	@Override
	public void print(Transaction tr, DBLock e) throws IOException,
			InterruptedException {
		((DiskIndex<K, T>) index).print(tr, e);
	}

	@Override
	public void print() {
		((IndexJava<K, T>) index).print();
	}

	@Override
	public void insertIndex(Transaction tr, Record<K, T> rec)
			throws IOException, InterruptedException {
		index.insert(tr, rec);
	}

	@Override
	public void deleteIndex(Transaction tr, K key) throws IOException,
			InterruptedException {
		index.delete(tr, key);
	}
}
