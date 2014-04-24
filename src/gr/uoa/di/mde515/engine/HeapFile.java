package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.index.DataFile;
import gr.uoa.di.mde515.index.Record;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public final class HeapFile<K extends Comparable<K>, V> extends DataFile<K, V> {

	private RandomAccessFile file;
	public static final byte[] BLANK_PAGE = new byte[Offsets.PAGE_SIZE];

	public HeapFile(String filename) {
		try {
			file = new RandomAccessFile(filename, "rw");
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Can't open database file " + filename,
				e);
		}
		// preallocateFile();
	}

	public Page readPage(int pageID) throws IOException {
		byte[] d = new byte[4096]; // normally, we use a buffer from the pool
		file.seek(pageID);
		file.read(d, 0, 4096);
		file.close();
		return new Page(pageID, d);
	}

	public void writePage(int pageID, byte[] data) throws IOException{
		file.seek(pageID);
		file.write(data);
		file.close();
	}


	// again the ByteBuffer could be used instead of seek
	public void deleteRecord(int pageID, int slot) {
		// file.seek(pageID+slot);
		// file.write();
	}

	public void searchForRecord(int pageID, int slot) throws IOException {
		file.seek(pageID + (slot * 12));//12 is record size
		System.out.println("The record is " + file.readInt()); // according to
																// the record
																// structure
	}

	public void synchPageToFile(Page apage) throws IOException {
		ByteBuffer data = apage.getData();
		int pageID = apage.getPageId();
		if (data != null) {
			file.seek(pageID);
			file.write(data.array());
		}
	}

	private void preallocateFile()
			throws IOException {
		for (long i = 0; i < Offsets.FILE_SIZE; i += Offsets.PAGE_SIZE) {
			file.write(BLANK_PAGE, 0, Offsets.PAGE_SIZE);
		}
		file.close();
	}

	@Override
	public void insert(Transaction tr, Record<K, V> rec) {
		lockHeader();
	}
	/*public static void main(String args[]) {
		try {
			MyFile obj = new MyFile();
			obj.createFileHeader();
			// Page a = obj.getPage(0);
			// System.out.println("(ByteBuffer)The number of records in the file is "+a.readInt(NUM_RECORDS_HEADER_LOCATION));
			// System.out.println("(ByteBuffer)The first data is  "+a.readInt(DATA_START_HEADER_LOCATION));
			// synchPageToFile(a);
			file.seek(0);
			System.out.println("The header value from file is "
				+ file.readInt());
			file.seek(4);
			System.out.println("The data value from file is " + file.readInt());
		} catch (IOException e) {
			System.err.println("Caught IOException df: " + e.getMessage());
		}
	}*/
	private void lockHeader() {}
}
