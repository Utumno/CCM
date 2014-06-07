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
	private final Serializer<K, V> ser;
	private final short header_size;

	public RecordsPage(int pageid, ByteBuffer dat, Serializer<K, V> ser,
			short header_size) {
		super(pageid, dat);
		this.ser = ser;
		this.header_size = header_size;
	}

	public RecordsPage(Page page, Serializer<K, V> ser, short header_size) {
		super(page);
		this.ser = ser;
		this.header_size = header_size;
	}

	// =========================================================================
	// Read/Write
	// =========================================================================
	public K readKey(int slot) { // TODO protected
		return ser.readKey(getDat(), slot, header_size);// TODO move
														// ser
														// up(avoid
														// getDat())
	}

	public V readValue(int slot) { // TODO protected
		return ser.readValue(getDat(), slot, header_size);
	}

	protected void writeKey(int slot, K key) {
		ser.writeKey(getDat(), slot, key, header_size);
	}

	protected void writeValue(int slot, V value) {
		ser.writeValue(getDat(), slot, value, header_size);
	}

	protected short getKeySize() {
		return ser.getKeySize();
	}

	protected short getRecordSize() {
		return ser.getRecordSize();
	}
}
