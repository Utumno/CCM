package gr.uoa.di.mde515.engine.buffer;

import gr.uoa.di.mde515.index.PageId;

import java.nio.ByteBuffer;

public class Page<T> {

	private PageId<T> pageid;
	private ByteBuffer dat;

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

	public void setPageId(PageId<T> pageid) {
		this.pageid = pageid;
	}

	public byte readByte(int pos) {
		return dat.get(pos);
	}

	public int readInt(int pos) {
		return dat.getInt(pos);
	}

	public short readShort(int pos) {
		return dat.getShort(pos);
	}

	public void writeShort(int pos, short value) {
		dat.putShort(pos, value);
	}

	public void writeByte(int pos, byte value) {
		dat.put(pos, value);
	}

	public void writeInt(int pos, int value) {
		dat.putInt(pos, value);
	}
}
