package gr.uoa.di.mde515.engine.buffer;

import java.nio.ByteBuffer;

public class Page {

	private int pageid;
	private ByteBuffer dat;
	public boolean dirty = false;

	public Page(int pageid, ByteBuffer dat) {
		this.pageid = pageid;
		this.dat = dat;
	}

	public ByteBuffer getData() {
		return dat;
	}

	public int getPageId() {
		return pageid;
	}

	public void setPageId(int pageid) {
		this.pageid = pageid;
	}

	public void setDirty() {
		dirty = true;
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
		setDirty();
		dat.putShort(pos, value);
	}

	public void writeByte(int pos, byte value) {
		setDirty();
		dat.put(pos, value);
	}

	public void writeInt(int pos, int value) {
		setDirty();
		dat.putInt(pos, value);
	}
}
