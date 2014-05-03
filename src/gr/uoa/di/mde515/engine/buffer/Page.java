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

	public void fileHeader(boolean isNew) {}

	/*
	 * public void setPageHeader(short value) { writeShort(PAGE_FREE_SPACE,
	 * (short) 100); // defined writeShort(PAGE_HEADER_NEXT, value); // ??
	 * writeShort(PAGE_HEADER_PREVIOUS, (short) 0); // ?? for (int i = 8; i <
	 * (Offsets.numSlots + 8); i++) { writeShort(i, (short) -1); }
	 * writeShort();// thesi tou prwtou record }
	 */
	/*
	 * void pageHeaderSetNext(short next) { writeShort(Offsets.PAGE_HEADER_NEXT,
	 * next); } void pageHeaderSetPrevious(short previous) {
	 * writeShort(Offsets.PAGE_HEADER_NEXT, previous); }
	 */
}
