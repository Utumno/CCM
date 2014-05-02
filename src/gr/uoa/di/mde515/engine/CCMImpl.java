package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.index.DataFile;
import gr.uoa.di.mde515.index.Index;
import gr.uoa.di.mde515.index.Index.KeyExistsException;
import gr.uoa.di.mde515.index.PageId;
import gr.uoa.di.mde515.index.Record;
import gr.uoa.di.mde515.locks.DBLock;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

final class CCMImpl<T> implements CCM {

	final List<Transaction> transactions = Collections
		.synchronizedList(new ArrayList<Transaction>()); // ...
	final Index<K, PageId<T>> index = new Index<>();

	// THREADS //
	private final ExecutorService exec;
	private static final int NUM_OF_THREADS = Runtime.getRuntime()
		.availableProcessors();
	private static final CCM INSTANCE = new CCMImpl<>();

	public CCMImpl() {
		exec = Executors.newFixedThreadPool(NUM_OF_THREADS);
	}

	private static abstract class DBoperation<R> implements Callable<R> {

		private final Transaction trans;

		DBoperation(Transaction trans) {
			if (trans == null)
				throw new NullPointerException("Null transaction.");
			this.trans = trans;
		}

		final Transaction getTrans() {
			return trans;
		}
	}

	private static abstract class DBRecordOperation<K extends Comparable<K>, V>
			extends DBoperation<Object> {

		@SuppressWarnings("unused")
		private final Record<K, V> rec;

		DBRecordOperation(Transaction trans, Record<K, V> rec) {
			super(trans);
			if (rec == null) throw new NullPointerException();
			this.rec = rec;
		}
	}

	/**
	 * <a href=
	 * "http://stackoverflow.com/questions/341971/what-is-the-execute-around-idiom"
	 * >Execute around </a> the crud operation supplied. Delegates to a worker
	 * thread from the pool while the current thread blocks waiting for the
	 * result of the db operation. TODO - add return types and generify
	 *
	 * @param crud
	 *            db operation to be performed
	 * @throws TransactionRequiredException
	 *             if no transaction was supplied
	 */
	private void _operate_(DBoperation<?> crud)
			throws TransactionRequiredException, ExecutionException {
		final Transaction trans = crud.getTrans(); // not null
		if (!transactions.contains(trans))
			throw new TransactionRequiredException();
		trans.validateThread();
		Future<?> submit = exec.submit(crud);
		try {
			submit.get(); // blocks
		} catch (InterruptedException e) {
			e.printStackTrace();
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public Transaction beginTransaction() {
		final Transaction tr = new Transaction();
		transactions.add(tr);
		return tr;
	}

	@Override
	public <K extends Comparable<K>, V> Record<K, V> insert(
			final Transaction tr, final Record<K, V> record,
			final DataFile<K, V> dataFile) throws TransactionRequiredException,
			ExecutionException {
		_operate_(new DBRecordOperation<K, V>(tr, record) {

			@Override
			public Record<K, V> call() throws KeyExistsException {
				index.lookupLocked(tr, record.getKey(), DBLock.E);
				dataFile.insert(tr, record);
				// V value = rec.getValue(); // this should now insert into the
				// file
				// if insertion to file is successful we must now insert into
				// the index and unlock
				throw new UnsupportedOperationException("Not supported yet."); // TODO
			}
		});
		return record;
	}

	@Override
	public void endTransaction(Transaction tr) {
		throw new UnsupportedOperationException("Not supported yet."); // TODO
	}

	@Override
	public <K extends Comparable<K>, V> Record<K, V> delete(Transaction tr,
			K key, DataFile<K, V> file) {
		throw new UnsupportedOperationException("Not supported yet."); // TODO
	}

	@Override
	public <K extends Comparable<K>, V> Record<K, V> lookup(Transaction tr,
			K key, DataFile<K, V> file) {
		throw new UnsupportedOperationException("Not supported yet."); // TODO
	}

	@Override
	public <K extends Comparable<K>, V> Record<K, V> update(Transaction tr,
			K key, DataFile<K, V> file) {
		throw new UnsupportedOperationException("Not supported yet."); // TODO
	}

	@Override
	public <K extends Comparable<K>, V> List<Record<K, V>> range(
			Transaction tr, K key1, K key2, DataFile<K, V> file) {
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

	public static <L extends Comparable<L>, M> CCM instance() {
		return INSTANCE;
	}
}
