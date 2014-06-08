package gr.uoa.di.mde515.engine.buffer;

import gr.uoa.di.mde515.index.Record;

import java.nio.ByteBuffer;

/**
 * Represents a slotted page. Each slot holds a key and a value, both of fixed
 * size. The page has a fixed size header.
 *
 * @param <K>
 *            the key type, must extend comparable
 * @param <V>
 *            the value type
 */
public abstract class RecordsPage<K extends Comparable<K>, V> extends Page {

	private final Serializer<K> serKey;
	private final Serializer<V> serVal;
	private final short header_size;
	// protected for subclasses to define the max keys value
	protected final int record_size;
	// mutable state (+ whatever inherited from Page !)
	protected volatile short numOfKeys; // policy for numOfKeys == 0

	protected abstract short getMax_keys();

	public RecordsPage(int pageid, ByteBuffer dat, Serializer<K> serKey,
			Serializer<V> serVal, short header_size) {
		super(pageid, dat);
		this.serKey = serKey;
		this.serVal = serVal;
		this.header_size = header_size;
		record_size = serKey.getTypeSize() + serVal.getTypeSize();
	}

	public RecordsPage(Page page, Serializer<K> serKey, Serializer<V> serVal,
			short header_size) {
		super(page);
		this.serKey = serKey;
		this.serVal = serVal;
		this.header_size = header_size;
		record_size = serKey.getTypeSize() + serVal.getTypeSize();
	}

	/**
	 * Returns the value with key {@code key} or {@code null} if no such key
	 * exists. Searches serially. TODO: public ?
	 */
	public V _get(K k) { // NON FINAL TODO: binary search in sorted files
		for (short i = 0; i < numOfKeys; ++i) {
			if (k.compareTo(readKey(i)) == 0) return readValue(i);
		}
		return null;
	}

	// =========================================================================
	// Final protected methods for a page containing records and a header
	// =========================================================================
	protected final boolean overflow() {
		return numOfKeys == getMax_keys(); // no more keys accepted
	}

	protected final K _lastKey() {
		if (numOfKeys == 0) return null;
		return readKey(numOfKeys - 1);
	}

	protected final K _firstKey() {
		if (numOfKeys == 0) return null;
		return readKey(0);
	}

	protected final Record<K, V> _lastPair() {
		if (numOfKeys == 0) return null;
		return new Record<>(readKey(numOfKeys - 1), readValue(numOfKeys - 1));
	}

	protected final Record<K, V> _firstPair() {
		if (numOfKeys == 0) return null;
		return new Record<>(readKey(0), readValue(0));
	}

	// =========================================================================
	// Read/Write
	// =========================================================================
	protected final K readKey(int slot) {
		return super.readType(header_size + slot * record_size, serKey);
	}

	protected final V readValue(int slot) {
		return super.readType(
			header_size + slot * record_size + serKey.getTypeSize(), serVal);
	}

	protected final void writeKey(int slot, K key) {
		super.writeType(header_size + slot * record_size, serKey, key);
	}

	protected final void writeValue(int slot, V value) {
		super.writeType(
			header_size + slot * record_size + serKey.getTypeSize(), serVal,
			value);
	}
}
