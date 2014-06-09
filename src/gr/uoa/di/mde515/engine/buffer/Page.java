package gr.uoa.di.mde515.engine.buffer;

import java.nio.ByteBuffer;

public class Page {

	private static final BufferManager buf = BufferManager.getInstance();
	// mutable state - oops private volatile boolean dirty; // TODO cache
	private final int pageid;
	private final ByteBuffer dat;

	/**
	 * Used by the buffer manager
	 *
	 * @param pageid
	 * @param dat
	 */
	Page(int pageid, ByteBuffer dat) {
		this.pageid = pageid;
		this.dat = dat;
	}

	/**
	 * Used by subclasses - passing in a page returned by the
	 * {@link BufferManager}.
	 *
	 * @param allocFrame
	 *            the allocated page by the BM - mapped to aframe
	 */
	protected Page(Page allocFrame) {
		this(allocFrame.pageid, allocFrame.dat);
	}

	public final int getPageId() {
		return pageid;
	}

	// =========================================================================
	// Read
	// =========================================================================
	public final <V> V readType(int offset, Serializer<V> ser) {
		return ser.readValue(dat, offset);
	}

	public final byte readByte(int pos) {
		return dat.get(pos);
	}

	public final int readInt(int pos) {
		return dat.getInt(pos);
	}

	public final short readShort(int pos) {
		return dat.getShort(pos);
	}

	// =========================================================================
	// Write
	// =========================================================================
	public final <V> void writeType(int offset, Serializer<V> ser, V value) {
		ser.writeValue(dat, offset, value);
		// if (!dirty) { // TODO cache - need to clean it too !
		buf.setPageDirty(pageid);
		// dirty = true;
	}

	public final void writeShort(int pos, short value) {
		dat.putShort(pos, value);
		buf.setPageDirty(pageid);
	}

	public final void writeByte(int pos, byte value) {
		dat.put(pos, value);
		buf.setPageDirty(pageid);
	}

	public final void writeInt(int pos, int value) {
		dat.putInt(pos, value);
		buf.setPageDirty(pageid);
	}

	// =========================================================================
	// Object Overrides
	// =========================================================================
	@Override
	public final String toString() {
		return "@" + getPageId();
	}

	@Override
	public final int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + pageid;
		return result;
	}

	@Override
	public final boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		Page other = (Page) obj;
		if (pageid != other.pageid) return false;
		return true;
	}
}
