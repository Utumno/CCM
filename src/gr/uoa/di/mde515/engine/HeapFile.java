package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.engine.buffer.BufferManager;
import gr.uoa.di.mde515.engine.buffer.Frame;
import gr.uoa.di.mde515.engine.buffer.Page;
import gr.uoa.di.mde515.index.DataFile;
import gr.uoa.di.mde515.index.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public final class HeapFile<K extends Comparable<K>, V> extends DataFile<K, V> {

	private BufferManager buf;
	private Header head;
	public static final byte[] BLANK_PAGE = new byte[4096];
	private static final int OFFSET_FREE_LIST = 0;
	private static final int OFFSET_FULL_LIST = 4;
	private static final int OFFSET_RECORD_SIZE = 8;
	private static final int OFFSET_PAGES_COUNT = 10;
	// PAGE HEADER OFFSETS
	private static final int NEXT_PAGE_OFFSET = 0;
	private static final int PREVIOUS_PAGE_OFFSET = 4;

	public HeapFile(String filename) {
		buf = BufferManager.getInstance();
		// file = new RandomAccessFile(filename, "rw");
		// KLEO
		// IF file existed:
		// bm.getHeader
		// lm. lockHeader for read (NO TRANSACTION)....
		// read Header and initialize _head_
		// ELSE init a new header and CREATE ONE BLANK PAGE
		// preallocateFile();
	}

	private final static class Header {

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
		// fileheader = file.readPage(0);
	}

	public void createFile(int numPages) throws IOException {
		createFileHeader();
		//createPageHeader(pageID);
	}

	public void createFileHeader() throws IOException {
		buf.allocateNewPage(0);
		Frame f = buf.allocFrame(0);
		Page p = new Page(0, f.getBufferFromFrame());
		p.writeInt(OFFSET_FREE_LIST, 1);
		p.writeInt(OFFSET_FULL_LIST, -1);
		p.writeShort(OFFSET_RECORD_SIZE, (short) 12);
		f.setDirty();
		// garbage collect page object
		p = null;
		try {
			buf.flushPage(0);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createPageHeader(int pageID) throws IOException {
		Frame f = buf.allocFrame(pageID);
		Page p = new Page(pageID, f.getBufferFromFrame());
		p.writeInt(NEXT_PAGE_OFFSET, -1);
		p.writeInt(PREVIOUS_PAGE_OFFSET, 0);
		f.setDirty();
		// garbage collect page object
		p = null;
		buf.flushPage(0);
	}

	// again the ByteBuffer could be used instead of seek
	public void deleteRecord(int pageID, int slot) {
		// file.seek(pageID+slot);
		// file.write();
	}

	public void searchForRecord(int pageID, int slot) throws IOException {
		//file.seek(pageID + (slot * 12));// 12 is record size
		//System.out.println("The record is " + file.readInt()); // according to
																// the record
																// structure
	}

	// private void preallocateFile() throws IOException {
	// for (long i = 0; i < Offsets.FILE_SIZE; i += Offsets.PAGE_SIZE) {
	// file.write(BLANK_PAGE, 0, Offsets.PAGE_SIZE);
	// }
	// file.close();
	// }
	@Override
	public void insert(Transaction tr, Record<K, V> rec) {
		//lockHeader();
		//PageId<Integer> p = nextAvailablePage();
		//Request<Integer> request = new LockManager.Request<>(p, tr, Lock.E);
		// lock manager request
	}

	//private PageId<Integer> nextAvailablePage() {
		//return new PageId<>(head.freeListStart);
	//}

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

	public static void main(String args[]){

	}
}


