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
		this(allocFrame.pageid, allocFrame.getDat());
	}

	public int getPageId() {
		return pageid;
	}

	ByteBuffer getDat() { // FIXME delete this
		return dat;
	}

	// =========================================================================
	// Read/Write
	// =========================================================================
	public byte readByte(int pos) {
		return getDat().get(pos);
	}

	public int readInt(int pos) {
		return getDat().getInt(pos);
	}

	public short readShort(int pos) {
		return getDat().getShort(pos);
	}

	// TODO !!!! consider adding buff.setPageDity(this) to the write calls
	public void writeShort(int pos, short value) {
		getDat().putShort(pos, value);
		// buff.setPageDirty(this); // buff is BufferManager<Integer> - I need
		// BufferManager<T>
	}

	public void writeByte(int pos, byte value) {
		getDat().put(pos, value);
	}

	public void writeInt(int pos, int value) {
		try {
			getDat().putInt(pos, value);
		} catch (IndexOutOfBoundsException e) {
			System.out.println("BOUND " + pos + " val " + value);
			throw e;
		}
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
