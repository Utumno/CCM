package gr.uoa.di.mde515.engine.buffer;

import java.nio.ByteBuffer;

public class Page { // TODO abstract

	// private static final BufferManager<Integer> buff = BufferManager
	// .getInstance();
	// FIXME immutable ?
	private final int pageid;
	private final ByteBuffer dat;

	public Page(int pageid, ByteBuffer dat) {
		this.pageid = pageid;
		this.dat = dat;
	}

	public Page(Page allocFrame) {
		this(allocFrame.pageid, allocFrame.dat);
	}

	public final int getPageId() {
		return pageid;
	}

	// =========================================================================
	// Read/Write
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

	// TODO !!!! consider adding buff.setPageDity(this) to the write calls
	public final <V> void writeType(int offset, Serializer<V> ser, V value) {
		ser.writeValue(dat, offset, value);
	}

	public final void writeShort(int pos, short value) {
		dat.putShort(pos, value);
	}

	public final void writeByte(int pos, byte value) {
		dat.put(pos, value);
	}

	public final void writeInt(int pos, int value) {
		dat.putInt(pos, value);
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
