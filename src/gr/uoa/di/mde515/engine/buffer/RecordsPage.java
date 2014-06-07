package gr.uoa.di.mde515.engine.buffer;

import gr.uoa.di.mde515.engine.Engine;

import java.nio.ByteBuffer;

/**
 * Represents a sloted page. Each slot holds a key and a value, both of fixed
 * size. The page has a fixed size header.
 *
 * @param <K>
 *            the key type, must extend comparable
 * @param <V>
 *            the value type
 */
public class RecordsPage<K extends Comparable<K>, V> extends Page {

	// private static final BufferManager<Integer> buff = BufferManager
	// .getInstance();
	// FIXME immutable ?
	private final Serializer<K> serKey;
	private final Serializer<V> serVal;
	private final short header_size;
	private final int record_size;
	private final short max_keys;

	protected final short getMax_keys() {
		return max_keys;
	}

	public RecordsPage(int pageid, ByteBuffer dat, Serializer<K> serKey,
			Serializer<V> serVal, short header_size) {
		super(pageid, dat);
		this.serKey = serKey;
		this.serVal = serVal;
		this.header_size = header_size;
		record_size = serKey.getTypeSize() + serVal.getTypeSize();
		max_keys = (short) ((Engine.PAGE_SIZE - header_size - serKey
			.getTypeSize()) / record_size);
	}

	public RecordsPage(Page page, Serializer<K> serKey, Serializer<V> serVal,
			short header_size) {
		super(page);
		this.serKey = serKey;
		this.serVal = serVal;
		this.header_size = header_size;
		record_size = serKey.getTypeSize() + serVal.getTypeSize();
		max_keys = (short) ((Engine.PAGE_SIZE - header_size - serKey
			.getTypeSize()) / record_size);
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
}
