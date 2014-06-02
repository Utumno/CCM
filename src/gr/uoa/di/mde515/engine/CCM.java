package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.engine.Engine.TransactionFailedException;
import gr.uoa.di.mde515.engine.Engine.TransactionalOperation;
import gr.uoa.di.mde515.files.DataFile;
import gr.uoa.di.mde515.index.Index;
import gr.uoa.di.mde515.index.KeyExistsException;
import gr.uoa.di.mde515.index.Record;
import gr.uoa.di.mde515.locks.DBLock;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Future;

public interface CCM {

	final class TransactionRequiredException extends Exception {

		private static final long serialVersionUID = 1451632646226806301L;

		public TransactionRequiredException() {
			super("This method must be called while in an active transaction.");
		}
	}

	Transaction beginTransaction();

	void endTransaction(Transaction tr);

	<K extends Comparable<K>, V, T> Record<K, V> insert(Transaction tr,
			Record<K, V> record, DataFile<K, V> file, final Index<K, T> index)
			throws TransactionRequiredException, KeyExistsException,
			TransactionFailedException;

	<K extends Comparable<K>, V, T> void delete(Transaction tr, K key,
			DBLock el, DataFile<K, V> file, final Index<K, T> index)
			throws IOException, InterruptedException,
			TransactionRequiredException,
			TransactionFailedException;

	<K extends Comparable<K>, V, T> Record<K, V> lookup(Transaction tr, K key,
			DBLock el, DataFile<K, V> dataFile, Index<K, T> index)
			throws IOException, InterruptedException;

	<K extends Comparable<K>, V> Record<K, V> update(Transaction tr, K key,
			DataFile<K, V> file);

	<K extends Comparable<K>, V> List<Record<K, V>> range(Transaction tr,
			K key1, K key2, DataFile<K, V> file);

	boolean waitTransaction(Transaction tr, long t);

	<K extends Comparable<K>, V> void abort(Transaction tr,
			DataFile<K, V> dataFile, Index<K, ?> index) throws IOException;

	void bulkLoad(Transaction tr, Path fileOfRecords);

	void bulkDelete(Transaction tr, Path fileOfKeys, Object newParam);

	void shutdown() throws InterruptedException;

	<K extends Comparable<K>, V> void commit(Transaction tr,
			DataFile<K, V> dataFile, Index<K, ?> index) throws IOException;

	<K extends Comparable<K>, V, T> Future submit(TransactionalOperation to);
}
