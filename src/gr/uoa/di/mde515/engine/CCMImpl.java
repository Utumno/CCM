package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.engine.Engine.TransactionFailedException;
import gr.uoa.di.mde515.files.DataFile;
import gr.uoa.di.mde515.index.Index;
import gr.uoa.di.mde515.index.KeyDoesntExistException;
import gr.uoa.di.mde515.index.KeyExistsException;
import gr.uoa.di.mde515.index.PageId;
import gr.uoa.di.mde515.index.Record;
import gr.uoa.di.mde515.locks.DBLock;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * All methods that require a Transaction will throw a
 * {@code NullPointerException} if the supplied transaction is {@code null} and
 * a {@link TransactionRequiredException} if the transaction supplied is not
 * valid. All exceptions from lower levels are wrapped in a
 * {@link TransactionFailedException}.
 */
enum CCMImpl implements CCM {
	INSTANCE;

	final List<Transaction> transactions = Collections
		.synchronizedList(new ArrayList<Transaction>()); // ...
	// THREADS //
	private final int NUM_OF_THREADS = 1/*
										 * Runtime.getRuntime()
										 * .availableProcessors()
										 */;
	private final ExecutorService exec = Executors
		.newFixedThreadPool(NUM_OF_THREADS);

	public static CCM instance() {
		return INSTANCE;
	}

	// =========================================================================
	// TransactionalOperation submit API
	// =========================================================================
	@Override
	public <K extends Comparable<K>, V, T, L> Future<L> submit(
			final Engine<K, V>.TransactionalOperation to) {
		return exec.submit(new Callable<L>() {

			@Override
			public L call() throws TransactionFailedException {
				Exception ex = null;
				to.init(); // the transaction is thread confined
				try {
					return to.execute();
				} catch (/* any old */Exception e) {
					try {
						to.abort();
					} catch (Exception e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					ex = e;
					throw e;
				} finally {
					if (ex == null) try {
						to.commit();
					} catch (/* any old */Exception e) {
						try {
							to.abort();
						} catch (Exception e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						throw e;
					} finally {
						to.endTransaction();
					}
				}
			}
		}); // I need to call submit.get() to have the ExecutionException thrown
	}

	@Override
	public <K extends Comparable<K>, V, T, L> List<Future<L>> submitAll(
			final Collection<Engine<K, V>.TransactionalOperation> tos)
			throws InterruptedException {
		Collection<Callable<L>> callables = new ArrayList<>();
		for (final Engine<K, V>.TransactionalOperation to : tos) {
			Callable<L> call = new Callable<L>() {

				@Override
				public L call() throws TransactionFailedException {
					Exception ex = null;
					to.init(); // the transaction is thread confined
					try {
						return to.execute();
					} catch (/* any old */Exception e) {
						to.abort();
						ex = e;
						throw e;
					} finally {
						if (ex != null) try {
							to.commit();
						} catch (/* any old */Exception e) {
							to.abort();
							throw e;
						} finally {
							to.endTransaction();
						}
					}
				}
			};
			callables.add(call);
		}
		return exec.invokeAll(callables); // TODO report to eclipse
		// invokeAll(callables) when callables is a Collection suggests to
		// replace with invokeAll(tasks,timeout,unit) which has the same problem
	}

	// =========================================================================
	// API
	// =========================================================================
	@Override
	public void shutdown() throws InterruptedException {
		INSTANCE.exec.shutdown();
		@SuppressWarnings("unused")
		boolean terminated = INSTANCE.exec.awaitTermination(13,
			TimeUnit.SECONDS); // if timed out terminated will be false
		// if (!terminated) {
		// List<Runnable> notExecuted = INSTANCE.exec.shutdownNow();
		// }
	}

	// =========================================================================
	// Active transaction - should be Package Private
	// =========================================================================
	@Override
	public Transaction beginTransaction() {
		final Transaction tr = new Transaction();
		transactions.add(tr);
		return tr;
	}

	@Override
	public <K extends Comparable<K>, V, T> Record<K, V> insert(
			final Transaction tr, final Record<K, V> record,
			final DataFile<K, V> dataFile, final Index<K, T> index)
			throws TransactionFailedException {
		if (record == null) throw new NullPointerException();
		_validate(tr);
		try {
			T lookupLocked = index.lookupLocked(tr, record.getKey(), DBLock.E);
			if (lookupLocked != null)
				throw new KeyExistsException("" + record.getKey());
			dataFile.lockHeader(tr, DBLock.E);
			PageId<T> pageID = dataFile.insert(tr, record);
			index.insert(tr, new Record<>(record.getKey(), pageID.getId()));
			return record; // NOOP
		} catch (IOException | InterruptedException | KeyExistsException e) {
			throw new TransactionFailedException(e);
		}
	}

	@Override
	public <K extends Comparable<K>, V, T> Record<K, V> lookup(
			final Transaction tr, final K key, final DBLock el,
			DataFile<K, V> dataFile, Index<K, T> index)
			throws TransactionFailedException {
		if (key == null) throw new NullPointerException();
		_validate(tr);
		try {
			T id = index.lookupLocked(tr, key, el);
			if (id == null) return null;
			return new Record<>(key, dataFile.get(tr, new PageId<>(id), key));
		} catch (IOException | InterruptedException e) {
			throw new TransactionFailedException(e);
		}
	}

	@Override
	public <K extends Comparable<K>, V, T> void delete(final Transaction tr,
			final K key, DBLock el, final DataFile<K, V> file,
			final Index<K, T> index) throws TransactionFailedException {
		if (key == null) throw new NullPointerException();
		_validate(tr);
		try {
			T lookupLocked = index.lookupLocked(tr, key, DBLock.E);
			if (lookupLocked == null)
				throw new KeyDoesntExistException("" + key);
			file.lockHeader(tr, DBLock.E);
			file.delete(tr, new PageId<>(lookupLocked), key);
			index.delete(tr, key);
		} catch (IOException | InterruptedException | KeyDoesntExistException e) {
			throw new TransactionFailedException(e);
		}
	}

	// =========================================================================
	// Committing
	// =========================================================================
	@Override
	public <K extends Comparable<K>, V> void commit(Transaction tr,
			DataFile<K, V> dataFile, Index<K, ?> index)
			throws TransactionFailedException {
		_validate(tr);
		try {
			tr.commit(dataFile, index);
		} catch (IOException e) {
			throw new TransactionFailedException(e);
		}
	}

	/** May throw IO when trying to reread a dirty permanent page - BAD */
	@Override
	public <K extends Comparable<K>, V> void abort(Transaction tr,
			DataFile<K, V> dataFile, Index<K, ?> index)
			throws TransactionFailedException {
		_validate(tr);
		try {
			tr.abort(dataFile, index);
		} catch (IOException e) {
			throw new TransactionFailedException(e);
		}
	}

	// =========================================================================
	// Ended
	// =========================================================================
	@Override
	public void endTransaction(Transaction tr) {
		tr.end();
		transactions.remove(tr);
	}

	// =========================================================================
	// Not implemented
	// =========================================================================
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
	public void bulkLoad(Transaction tr, Path fileOfRecords) {
		throw new UnsupportedOperationException("Not implemented"); // TODO
	}

	@Override
	public void bulkDelete(Transaction tr, Path fileOfKeys, Object newParam) {
		throw new UnsupportedOperationException("Not implemented"); // TODO
	}

	// =========================================================================
	// Helpers
	// =========================================================================
	private void _validate(Transaction tr) {
		if (!transactions.contains(tr))
			throw new RuntimeException(new TransactionRequiredException());
		tr.validateThread();
	}
}
