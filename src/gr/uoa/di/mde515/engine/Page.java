package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.index.PageId;

import java.nio.ByteBuffer;

public class Page<T> {

	// OFSSETS
	private static final short NEXT_PAGE_OFFSET = 0;
	private static final short PREVIOUS_PAGE_OFFSET = 4;
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

	public void fileHeader(boolean isNew) {}

	public void setPageHeader(short value) {
		writeShort(PAGE_FREE_SPACE, (short) 100); // defined
		writeShort(PAGE_HEADER_NEXT, value); // ??
		writeShort(PAGE_HEADER_PREVIOUS, (short) 0); // ??
		for (int i = 8; i < (Offsets.numSlots + 8); i++) {
			writeShort(i, (short) -1);
		}
		writeShort();// thesi tou prwtou record
	}

	void pageHeaderSetNext(short next) {
		writeShort(Offsets.PAGE_HEADER_NEXT, next);
	}

	void pageHeaderSetPrevious(short previous) {
		writeShort(Offsets.PAGE_HEADER_NEXT, previous);
	}

	public static void main(String args[]) throws Exception {
		int[] array = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
				17, 18, 19, 20 };
		Page obj = new Page(0, BLANK_PAGE);
		obj.setDirty();
		System.out.println("The page id is " + obj.pageid);
		System.out.println("The dirty field is " + obj.dirty);
		ByteBuffer buf = obj.getData();
		obj.writeInt(0, 5);
		boolean a = buf.hasArray();
		System.out.println("The buffer is backed? " + a);
		buf.flip();
		while (buf.hasRemaining()) {
			System.out.println(buf.getInt());
		}
		// preallocateTestFile(FILE_NAME);
	}
}
