package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.index.PageId;

import java.nio.ByteBuffer;

public class Page<T> {

	// OFSSETS
	private static final short NEXT_PAGE_OFFSET = 0;
	private static final short PREVIOUS_PAGE_OFFSET = 4;
	public static final byte[] BLANK_PAGE = new byte[4096];
	// /OFFSETS
	private PageId<T> pageid;
	// private int numSlots;
	// private short[] slots;
	private int previousPage;
	private int nextPage;
	private ByteBuffer dat;
	public boolean dirty = false;
	public static final String FILE_NAME = "customers.txt";
	//
	int numSlots = 100;
	int NUM_RECORDS_HEADER_LOCATION = 0;
	int DATA_START_HEADER_LOCATION = 4;

	// sug: instead we could accept the ByteBuffer immediately
	public Page(T pageid, byte[] data) {
		this.pageid = new PageId<>(pageid);
		this.dat = ByteBuffer.wrap(data);
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
