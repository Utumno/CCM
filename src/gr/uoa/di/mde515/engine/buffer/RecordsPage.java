package gr.uoa.di.mde515.engine.buffer;

import java.nio.ByteBuffer;

// TODO - implement Serializer ?
/**
 * Represents a sloted page of keys and values with fixed size values and keys
 * and a fixed size header.
 *
 * @param <K>
 *            the key type
 * @param <V>
 *            the value
 * @param <T>
 *            the type of the PageId<T>
 */
public class RecordsPage<K extends Comparable<K>, V, T> extends Page {

	// private static final BufferManager<Integer> buff = BufferManager
	// .getInstance();
	// FIXME immutable ?
	private final Serializer<K> serKey;
	private final Serializer<V> serVal;
	private final short header_size;
	private final int record_size;

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

	// =========================================================================
	// Read/Write
	// =========================================================================
	public K readKey(int slot) { // TODO protected
		return serKey.readValue(getDat(), header_size + slot * record_size);
		// TODO move ser up(avoid getDat())
	}

	public V readValue(int slot) { // TODO protected
		return serVal.readValue(getDat(), header_size + slot * record_size
			+ serKey.getTypeSize());
	}

	protected void writeKey(int slot, K key) {
		serKey.writeValue(getDat(), header_size + slot * record_size, key);
	}

	protected void writeValue(int slot, V value) {
		serVal.writeValue(getDat(),
			header_size + slot * record_size + serKey.getTypeSize(), value);
	}

	protected short getKeySize() {
		return serKey.getTypeSize();
	}

	protected final int getRecordSize() {
		return record_size;
	}
}
