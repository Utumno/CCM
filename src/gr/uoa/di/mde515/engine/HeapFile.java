package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.index.DataFile;
import gr.uoa.di.mde515.index.PageId;
import gr.uoa.di.mde515.index.Record;
import gr.uoa.di.mde515.locks.Lock;
import gr.uoa.di.mde515.locks.LockManager;
import gr.uoa.di.mde515.locks.LockManager.Request;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public final class HeapFile<K extends Comparable<K>, V> extends DataFile<K, V> {

	private RandomAccessFile file;
	private Header head;
	public static final byte[] BLANK_PAGE = new byte[Header.PAGE_SIZE];

	public HeapFile(String filename) {
		try {
			file = new RandomAccessFile(filename, "rw");
			// KLEO
			// IF file existed:
			// bm.getHeader
			// lm. lockHeader for read (NO TRANSACTION)....
			// read Header and initialize _head_
			// ELSE init a new header and CREATE ONE BLANK PAGE
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Can't open database file " + filename,
				e);
		}
		// preallocateFile();
	}

	private final static class Header {

		private static final short PAGE_SIZE = 4096; // TODO move to globals
		private static final short OFFSET_FREE_LIST = 0;
		private static final short OFFSET_FULL_LIST = 4;
		private static final short OFFSET_RECORD_SIZE = 8;
		private static final short OFFSET_PAGES_COUNT = 10;
		private int freeListStart;
		private int fullListStart;
		private int numPages;
		final short RECORD_SIZE;

		public Header(Page header) {
			freeListStart = header.readInt(OFFSET_FREE_LIST);
			fullListStart = header.readInt(OFFSET_FULL_LIST);
			RECORD_SIZE = header.readShort(OFFSET_RECORD_SIZE);
			numPages = header.readInt(OFFSET_PAGES_COUNT);
			try {
				Path path = Paths.get("/home/temp/", "hugefile.txt");
				SeekableByteChannel sbc = Files.newByteChannel(path,
					StandardOpenOption.READ);
				ByteBuffer bf = ByteBuffer.allocate(941);// line size
				int i = 0;
				while ((i = sbc.read(bf)) > 0) {
					bf.flip();
					System.out.println(new String(bf.array()));
					bf.clear();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public Header(short recordSize) {
			RECORD_SIZE = recordSize;
			freeListStart = 1;
			fullListStart = -1;
			numPages = 1;
		}
		// fileheader = file.readPage(0);
	}

	public Page readPage(int pageID) throws IOException {
		byte[] d = new byte[4096]; // normally, we use a buffer from the pool
		try {
			file.seek(pageID * head.PAGE_SIZE);
			file.read(d, 0, 4096);
		} finally {
			try {
				file.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return new Page(pageID, d);
	}

	public void writePage(int pageID, byte[] data) throws IOException {
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
		file.seek(pageID + (slot * 12));// 12 is record size
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

	// private void preallocateFile() throws IOException {
	// for (long i = 0; i < Offsets.FILE_SIZE; i += Offsets.PAGE_SIZE) {
	// file.write(BLANK_PAGE, 0, Offsets.PAGE_SIZE);
	// }
	// file.close();
	// }
	@Override
	public void insert(Transaction tr, Record<K, V> rec) {
		lockHeader();
		PageId<Integer> p = nextAvailablePage();
		Request<Integer> request = new LockManager.Request<>(p, tr, Lock.E);
		// lock manager request
	}

	private PageId<Integer> nextAvailablePage() {
		return new PageId<>(head.freeListStart);
	}

	// public static void main(String args[]) {
	// try {
	// MyFile obj = new MyFile();
	// obj.createFileHeader(); // Page a = obj.getPage(0); //
	// System.out
	// .println("(ByteBuffer)The number of records in the file is "
	// + a.readInt(NUM_RECORDS_HEADER_LOCATION)); //
	// System.out.println("(ByteBuffer)The first data is  "
	// + a.readInt(DATA_START_HEADER_LOCATION)); // synchPageToFile(a);
	// file.seek(0);
	// System.out.println("The header value from file is "
	// + file.readInt());
	// file.seek(4);
	// System.out.println("The data value from file is " + file.readInt());
	// } catch (IOException e) {
	// System.err.println("Caught IOException df: " + e.getMessage());
	// }
	// }
	private void lockHeader() {
		throw new UnsupportedOperationException("Not implemented"); // TODO
	}
}
