package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.engine.buffer.IntegerSerializer;
import gr.uoa.di.mde515.engine.buffer.Serializer;
import gr.uoa.di.mde515.files.DataFile;
import gr.uoa.di.mde515.files.IndexDiskFile;
import gr.uoa.di.mde515.index.DiskIndex;
import gr.uoa.di.mde515.index.Index;
import gr.uoa.di.mde515.index.IndexJava;
import gr.uoa.di.mde515.index.Record;
import gr.uoa.di.mde515.locks.DBLock;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Represents the DB external interface. It is a monofilestic engine but can be
 * extended to handle more files, by making the methods instead of the class
 * generic (for added complexity). The single file has a single (dense) index.
 * The pages of the files are stamped with an integer id, which is negative for
 * the pages of the index file. Should be an interface implemented by enums (for
 * singleton property) but with the addition of a static factory (TODO: java 8
 * ?). <br/>
 * preAlpha<br/>
 * FIXME: The current Heap file implementation expects Integers (there are casts
 * that will blow if not) - generify
 *
 * @param <K>
 *            the key type of the records in the one and only one file. Must
 *            extend {@link Comparable}
 * @param <V>
 *            the type of the records value - that is all the attributes except
 *            the key. No restrictions here.
 */
public abstract class Engine<K extends Comparable<K>, V> {

	public static final short PAGE_SIZE = 48;
	// TODO fix this mess - proper factory - java 8 ?
	private static volatile Engine<?, ?> instance;
	private final static Object HACK = new Object();

	// =========================================================================
	// Engine API
	// =========================================================================
	/**
	 * Execute a TransactionalOperation in its own thread - the
	 * TransactionalOperation instance is not to be reused afterwards. Takes
	 * care of unlocking the locks held by the transaction.
	 *
	 * Implementation: calls submit on an {@link ExecutorService}
	 *
	 * @param to
	 *            the TransactionalOperation to be executed
	 * @return a Future with the {@link TransactionalOperation#execute()} result
	 */
	public abstract <L> Future<L> submit(TransactionalOperation to);

	/**
	 * Execute a collection TransactionalOperation
	 * <em> each in its own thread</em>, as a separate transaction - the
	 * TransactionalOperation instances are not to be reused afterwards. Takes
	 * care of unlocking the locks held by the transaction.
	 *
	 * Implementation: calls {@link ExecutorService#invokeAll(Collection)} on an
	 * ExecutorService. TODO - call invokeAll(tasks, timeout, unit)
	 *
	 * @param tos
	 *            the Collection of the TransactionalOperation to be executed
	 * @throws InterruptedException
	 *             thrown by {@link ExecutorService#invokeAll(Collection)}
	 */
	public abstract <L> List<Future<L>> submitAll(
			Collection<TransactionalOperation> tos) throws InterruptedException;

	public static <K extends Comparable<K>, V> Engine<K, V> newInstance(
			Serializer<K> serKey, Serializer<V> serVal) {
		if (instance == null)
			synchronized (HACK) {
				if (instance == null) {
					final EngineImpl<K, V> engineImpl = new EngineImpl<>(
						serKey, serVal);
					instance = engineImpl;
					return engineImpl;
				}
			}
		throw new IllegalStateException("Engine already initialized");
	}

	public abstract void shutdown() throws InterruptedException, IOException;

	/**
	 * The clients must override the execute method of this class then submit
	 * the TransactionalOperation instance(s). Each TransactionalOperation will
	 * be executed inside a transaction. FIXME: make TO a functional iface ?
	 */
	public abstract class TransactionalOperation {

		Transaction trans; // should be final ! make sure it's thread confined

		// =====================================================================
		// TransactionalOperation API
		// =====================================================================
		public abstract <L> L execute() throws TransactionFailedException;

		public final Record<K, V> lookup(K key, DBLock e)
				throws TransactionFailedException {
			return Engine.this.lookup(trans, key, e);
		}

		public final Record<K, V> insert(Record<K, V> record)
				throws TransactionFailedException {
			return Engine.this.insert(trans, record);
		}

		public final void delete(K in, DBLock e)
				throws TransactionFailedException {
			Engine.this.delete(trans, in, e);
		}

		/** ONLY FOR DEBUG */
		protected final void print(DBLock el) throws TransactionFailedException {
			try {
				Engine.this.print(trans, el);
			} catch (IOException | InterruptedException e) {
				throw new TransactionFailedException(e);
			}
		}

		/** ONLY FOR DEBUG */
		protected final void insertIndex(Record<K, Integer> rec)
				throws TransactionFailedException {
			try {
				Engine.this.insertIndex(trans, rec);
			} catch (IOException | InterruptedException e) {
				throw new TransactionFailedException(e);
			}
		}

		// =====================================================================
		// Package private - Transaction management
		// =====================================================================
		final void init() {
			trans = beginTransaction();
		}

		final void endTransaction() {
			Engine.this.endTransaction(trans);
		}

		final void abort() throws TransactionFailedException {
			Engine.this.abort(trans);
		}

		final void commit() throws TransactionFailedException {
			Engine.this.commit(trans);
		}
	}

	public static final class TransactionFailedException extends Exception {

		private static final long serialVersionUID = -4298165326203675694L;

		public TransactionFailedException(Exception e) {
			super("Transaction operation failed", e);
		}
	}

	// =========================================================================
	// Abstract Package private methods - Delegate to Engine implementation
	// =========================================================================
	abstract Transaction beginTransaction();

	abstract Record<K, V> lookup(Transaction tr, K key, DBLock el)
			throws TransactionFailedException; // TODO boolean lookup

	abstract Record<K, V> insert(Transaction tr, Record<K, V> record)
			throws TransactionFailedException;

	abstract void delete(Transaction tr, K key, DBLock el)
			throws TransactionFailedException;

	abstract void waitTransaction(Transaction tr, long time)
			throws InterruptedException;

	abstract void commit(Transaction tr) throws TransactionFailedException;

	abstract void abort(Transaction tr) throws TransactionFailedException;

	abstract void endTransaction(Transaction tr);

	// Record<K,V> update(T key);
	// List<Record<K,V>> range(T key1, T key2);
	// File bulk_load(File fileOfRecords);
	// File bulk_delete(File fileOfKeys);
	// =========================================================================
	// Abstract debug methods
	// =========================================================================
	/** ONLY FOR DEBUG */
	public abstract void deleteIndex(Transaction tr, K rec) throws IOException,
			InterruptedException;

	/** ONLY FOR DEBUG */
	public abstract void print();

	/** ONLY FOR DEBUG */
	public abstract void insertIndex(Transaction tr, Record<K, Integer> rec)
			throws IOException, InterruptedException;

	/** ONLY FOR DEBUG _ WILL LOCK THE WHOLE TREE */
	public abstract void print(Transaction tr, DBLock e) throws IOException,
			InterruptedException;
}

final class EngineImpl<K extends Comparable<K>, V> extends Engine<K, V> {

	private static final String DB_FILE = "db.db";
	private static final String INDEX_FILE = "index.db";
	private final CCM ccm;
	private final DataFile<K, V> dataFile;
	private final Index<K, Integer> index;

	EngineImpl(Serializer<K> serKey, Serializer<V> serVal/* TODO in heap */) {
		this.ccm = CCMImpl.instance();
		String opening = DB_FILE;
		try {
			dataFile = DataFile.init(opening, serKey, serVal);
			opening = INDEX_FILE;
			index = new DiskIndex<>(new IndexDiskFile(opening), serKey,
				IntegerSerializer.INSTANCE);
			System.out.println("ENGINE INITIALIZED");
		} catch (IOException | InterruptedException e) {
			throw new RuntimeException("Can't open " + opening + " file", e);
		}
	}

	@Override
	public <L> Future<L> submit(Engine<K, V>.TransactionalOperation to) {
		return ccm.submit(to);
	}

	@Override
	public <L> List<Future<L>> submitAll(
			Collection<Engine<K, V>.TransactionalOperation> to)
			throws InterruptedException {
		return ccm.submitAll(to);
	}

	@Override
	public void shutdown() throws InterruptedException, IOException {
		ccm.shutdown();
		dataFile.close();
	}

	// =========================================================================
	// Package private - Delegates to CCM
	// =========================================================================
	@Override
	Transaction beginTransaction() {
		return ccm.beginTransaction();
	}

	@Override
	Record<K, V> lookup(Transaction tr, K key, DBLock el)
			throws TransactionFailedException {
		return ccm.lookup(tr, key, el, dataFile, index);
	}

	@Override
	Record<K, V> insert(Transaction tr, Record<K, V> record)
			throws TransactionFailedException {
		return ccm.insert(tr, record, dataFile, index);
	}

	@Override
	void delete(Transaction tr, K key, DBLock el)
			throws TransactionFailedException {
		ccm.delete(tr, key, el, dataFile, index);
	}

	@Override
	void waitTransaction(Transaction tr, long time) throws InterruptedException {
		Thread.sleep(time); // FIXME disallow waits after commit/abort
	}

	@Override
	void commit(Transaction tr) throws TransactionFailedException {
		ccm.commit(tr, dataFile, index);
	}

	@Override
	void abort(Transaction tr) throws TransactionFailedException {
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
		((DiskIndex<K, Integer>) index).print(tr, e);
	}

	@Override
	public void print() {
		((IndexJava<K, Integer>) index).print();
	}

	@Override
	public void insertIndex(Transaction tr, Record<K, Integer> rec)
			throws IOException, InterruptedException {
		index.insert(tr, rec);
	}

	@Override
	public void deleteIndex(Transaction tr, K key) throws IOException,
			InterruptedException {
		index.delete(tr, key);
	}
}
