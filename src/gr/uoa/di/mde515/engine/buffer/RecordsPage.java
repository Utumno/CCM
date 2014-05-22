package gr.uoa.di.mde515.engine.buffer;

import java.nio.ByteBuffer;

// TODO - implement serializer ?
public class RecordsPage<K extends Comparable<K>, V, T> extends Page<T> {

	// private static final BufferManager<Integer> buff = BufferManager
	// .getInstance();
	// FIXME immutable ?
	private final Serializer<K, V> ser;

	public RecordsPage(T pageid, ByteBuffer dat, Serializer<K, V> ser) {
		super(pageid, dat);
		this.ser = ser;
	}

	public RecordsPage(Page<T> page, Serializer<K, V> ser) {
		super(page);
		this.ser = ser;
	}

	// =========================================================================
	// Read/Write
	// =========================================================================
	public K readKey(int offset) {
		return ser.readKey(getDat(), offset);// TODO move ser up(avoid getDat())
	}

	public V readValue(int offset) {
		return ser.readValue(getDat(), offset);
	}

	protected void writeKey(int offset, K key) {
		ser.writeKey(getDat(), offset, key);
	}

	protected void writeValue(int offset, V value) {
		ser.writeValue(getDat(), offset, value);
	}
}
