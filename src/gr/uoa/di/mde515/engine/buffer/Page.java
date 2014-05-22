package gr.uoa.di.mde515.engine.buffer;

import gr.uoa.di.mde515.index.PageId;

import java.nio.ByteBuffer;

public class Page<T> { // TODO abstract

	// private static final BufferManager<Integer> buff = BufferManager
	// .getInstance();
	// FIXME immutable ?
	private final PageId<T> pageid;
	private final ByteBuffer dat;

	public Page(T pageid, ByteBuffer dat) {
		this.pageid = new PageId<>(pageid);
		this.dat = dat;
	}

	public Page(Page<T> allocFrame) {
		this(allocFrame.pageid.getId(), allocFrame.getDat());
	}

	public PageId<T> getPageId() {
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

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Page [pageid=");
		builder.append(pageid);
		builder.append(", dat=");
		builder.append(getDat());
		builder.append("]");
		return builder.toString();
	}
}
