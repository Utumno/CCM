package gr.uoa.di.mde515.engine.buffer;

import gr.uoa.di.mde515.index.PageId;

import java.nio.ByteBuffer;

public class Page<T> {

	// private static final BufferManager<Integer> buff = BufferManager
	// .getInstance();
	// FIXME immutable
	private final PageId<T> pageid;
	private final ByteBuffer dat;

	public Page(PageId<T> pageid, ByteBuffer dat) {
		this.pageid = pageid;
		this.dat = dat;
	}

	public Page(Page<T> allocFrame) {
		this(allocFrame.pageid, allocFrame.dat);
	}

	public ByteBuffer getData() {
		return dat;
	}

	public PageId<T> getPageId() {
		return pageid;
	}

	// =========================================================================
	// Read/Write
	// =========================================================================
	public byte readByte(int pos) {
		return dat.get(pos);
	}

	public int readInt(int pos) {
		return dat.getInt(pos);
	}

	public short readShort(int pos) {
		return dat.getShort(pos);
	}

	// TODO !!!! consider adding buff.setPageDity(this) to the write calls
	public void writeShort(int pos, short value) {
		dat.putShort(pos, value);
		// buff.setPageDirty(this); // buff is BufferManager<Integer> - I need
		// BufferManager<T>
	}

	public void writeByte(int pos, byte value) {
		dat.put(pos, value);
	}

	public void writeInt(int pos, int value) {
		dat.putInt(pos, value);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Page [pageid=");
		builder.append(pageid);
		builder.append(", dat=");
		builder.append(dat);
		builder.append("]");
		return builder.toString();
	}
}
