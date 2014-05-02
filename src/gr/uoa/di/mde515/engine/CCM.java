package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.index.DataFile;
import gr.uoa.di.mde515.index.Index.KeyExistsException;
import gr.uoa.di.mde515.index.Record;

import java.io.File; // FIXME
import java.util.List;
import java.util.concurrent.ExecutionException;

public interface CCM {

	final class TransactionRequiredException extends Exception {

		private static final long serialVersionUID = 1451632646226806301L;

		public TransactionRequiredException() {
			super("This method must be called while in an active transaction.");
		}
	}

	Transaction beginTransaction();

	void endTransaction(Transaction tr);

	<K extends Comparable<K>, V> Record<K, V> insert(Transaction tr,
			Record<K, V> record, DataFile<K, V> file)
			throws TransactionRequiredException, KeyExistsException,
			ExecutionException;

	<K extends Comparable<K>, V> Record<K, V> delete(Transaction tr, K key,
			DataFile<K, V> file);

	<K extends Comparable<K>, V> Record<K, V> lookup(Transaction tr, K key,
			DataFile<K, V> file);

	<K extends Comparable<K>, V> Record<K, V> update(Transaction tr, K key,
			DataFile<K, V> file);

	<K extends Comparable<K>, V> List<Record<K, V>> range(Transaction tr,
			K key1, K key2, DataFile<K, V> file);

	boolean waitTransaction(Transaction tr, long t);

	void abort(Transaction tr);

	void commit(Transaction tr);

	File bulkLoad(Transaction tr, File fileOfRecords);

	File bulkDelete(Transaction tr, File fileOfKeys);
}
