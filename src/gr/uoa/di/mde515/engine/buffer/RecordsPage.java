package gr.uoa.di.mde515.engine.buffer;

import gr.uoa.di.mde515.engine.Engine;

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
public class RecordsPage<K extends Comparable<K>, V> extends Page {

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
