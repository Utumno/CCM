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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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

	public static final short PAGE_SIZE = 48;
	private static volatile Engine<?, ?, ?> instance;
	private final static Object HACK = new Object(); // TODO fix this mess

	/**
	 * The clients must override the execute method of this class then submit
	 * the TransactionalOperation instance(s). Each TransactionalOperation will
	 * be executed inside a transaction.
	 */
	public abstract class TransactionalOperation {

		Transaction trans; // should be final ! make sure it's thread confined

		/** FIXME: remove exceptions and make it to a functional iface */
		public abstract void execute() throws InterruptedException,
				IOException, TransactionRequiredException, KeyExistsException,
				TransactionFailedException, ExecutionException;

		public final Record<K, V> lookup(K key, DBLock e) throws IOException,
				InterruptedException {
			return Engine.this.lookup(trans, key, e);
		}

		public final Record<K, V> insert(Record<K, V> record)
				throws TransactionRequiredException, KeyExistsException,
				TransactionFailedException {
			return Engine.this.insert(trans, record);
		}

		public final void delete(K in, DBLock e) throws IOException,
				InterruptedException, TransactionRequiredException,
				TransactionFailedException {
			Engine.this.delete(trans, in, e);
		}

		public final void abort() throws IOException {
			Engine.this.abort(trans);
		}

		public final void commit() throws IOException {
			Engine.this.commit(trans);
		}

		protected void print(DBLock e) throws IOException, InterruptedException {
			Engine.this.print(trans, e);
		}

		protected void insertIndex(Record<K, T> rec) throws IOException,
				InterruptedException {
			Engine.this.insertIndex(trans, rec);
		}

		// =====================================================================
		// Package private - Transaction management
		// =====================================================================
		void init() {
			trans = beginTransaction();
		}

		void endTransaction() {
			Engine.this.endTransaction(trans);
		}
	}

	public static final class TransactionFailedException extends Exception {

		private static final long serialVersionUID = -4298165326203675694L;

		public TransactionFailedException(Exception e) {
			super("Transaction operation failed", e);
		}
	}

	public abstract Future submit(TransactionalOperation to);

	public abstract <L> List<Future<L>> submitAll(
			Collection<TransactionalOperation> to) throws InterruptedException;

	public static <K extends Comparable<K>, V, T> Engine<?, ?, ?> newInstance(
			Serializer<K, T> ser) {
		if (instance == null) synchronized (HACK) {
			if (instance == null) instance = new EngineImpl<K, V, T>(ser);
		}
		return instance;
	}

	public abstract void shutdown() throws InterruptedException, IOException;

	// =========================================================================
	// Abstract Package private methods
	// =========================================================================
	abstract Transaction beginTransaction();

	abstract void endTransaction(Transaction tr);

	abstract Record<K, V> insert(Transaction tr, Record<K, V> record)
			throws TransactionRequiredException, KeyExistsException,
			TransactionFailedException;

	abstract void commit(Transaction tr) throws IOException;

	abstract void waitTransaction(Transaction tr, long time)
			throws InterruptedException;

	abstract void abort(Transaction tr) throws IOException;

	abstract void delete(Transaction tr, K key, DBLock el) throws IOException,
			InterruptedException, TransactionRequiredException,
			TransactionFailedException;

	abstract Record<K, V> lookup(Transaction tr, K key, DBLock el)
			throws IOException, InterruptedException; // TODO boolean lookup
	// Record<K,V> update(T key);
	// List<Record<K,V>> range(T key1, T key2);
	// File bulk_load(File fileOfRecords);
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
	public Future submit(Engine<K, V, T>.TransactionalOperation to) {
		return ccm.submit(to);
	}

	@Override
	public <L> List<Future<L>> submitAll(
			Collection<Engine<K, V, T>.TransactionalOperation> to)
			throws InterruptedException {
		return ccm.submitAll(to);
	}

	@Override
	public void shutdown() throws InterruptedException, IOException {
		ccm.shutdown();
		dataFile.close();
	}

	// =========================================================================
	// Package private
	// =========================================================================
	@Override
	Transaction beginTransaction() {
		return ccm.beginTransaction();
	}

	@Override
	Record<K, V> insert(Transaction tr, Record<K, V> record)
			throws TransactionRequiredException, KeyExistsException,
			TransactionFailedException {
		return ccm.insert(tr, record, dataFile, index);
	}

	@Override
	Record<K, V> lookup(Transaction tr, K key, DBLock el) throws IOException,
			InterruptedException {
		return ccm.lookup(tr, key, el, dataFile, index);
	}

	@Override
	void delete(Transaction tr, K key, DBLock el) throws IOException,
			InterruptedException, TransactionRequiredException,
			TransactionFailedException {
		ccm.delete(tr, key, el, dataFile, index);
	}

	@Override
	void waitTransaction(Transaction tr, long time) throws InterruptedException {
		Thread.sleep(time); // FIXME disallow waits after commit/abort
	}

	@Override
	void commit(Transaction tr) throws IOException {
		ccm.commit(tr, dataFile, index);
	}

	@Override
	void abort(Transaction tr) throws IOException {
		ccm.abort(tr, dataFile, index);
	}

	@Override
	void endTransaction(Transaction tr) {
		ccm.endTransaction(tr);
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
