package gr.uoa.di.mde515.engine.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;

import gr.uoa.di.mde515.index.*;

public class HF {

	private BufferManager buf;
	// useful constants
	private static final int PAGE_SIZE = 32; // TODO move to globals
	private static final int RECORD_SIZE = 8;
	// it the size of entry in the fileheader
	private static final int ENTRY_SIZE = 4;
	private static final int FILEHEADER_LENGTH = 6;
	// maximum number of slots in a page
	private static final int OFFSET_PAGES_COUNT = 0;
	private static final int OFFSET_RECORD_SIZE = 4;
	// PAGE HEADER OFFSETS
	// Instead of linked list heap file, I changed to directory of pages
	// maximum # of slots in page
	private static final int MAXIMUM_NUMBER_OF_SLOTS = 2;
	private static final int OFFSET_CURRENT_PAGE = 0;
	private static final int OFFSET_NEXT_FREE_SLOT = 4;
	private static final int OFFSET_CURRENT_NUMBER_OF_SLOTS = 8;

	public HF() {
		buf = BufferManager.getInstance();
	}

	/*
	 * private final static class Header { private int freeListStart; private
	 * int fullListStart; private int numPages; final short RECORD_SIZE; public
	 * Header(Page header) { freeListStart = header.readInt(OFFSET_FREE_LIST);
	 * fullListStart = header.readInt(OFFSET_FULL_LIST); RECORD_SIZE =
	 * header.readShort(OFFSET_RECORD_SIZE); numPages =
	 * header.readInt(OFFSET_PAGES_COUNT); try { Path path =
	 * Paths.get("/home/temp/", "hugefile.txt"); SeekableByteChannel sbc =
	 * Files.newByteChannel(path, StandardOpenOption.READ); ByteBuffer bf =
	 * ByteBuffer.allocate(941);// line size int i = 0; while ((i =
	 * sbc.read(bf)) > 0) { bf.flip(); System.out.println(new
	 * String(bf.array())); bf.clear(); } } catch (Exception e) {
	 * e.printStackTrace(); } } }
	 */
	public BufferManager buf() {
		return buf;
	}

	/**
	 * Creates the file header
	 * 
	 * @throws IOException
	 */
	public void createFileHeader() throws IOException {
		buf.newPage(0);
		Frame f = buf.allocFrame(0);
		Page p = new Page(0, f.getBufferFromFrame());
		p.writeInt(OFFSET_PAGES_COUNT, 0);
		// p.writeInt(OFFSET_FREE_LIST, 1);
		// p.writeInt(OFFSET_FULL_LIST, -1);
		p.writeShort(OFFSET_RECORD_SIZE, (short) RECORD_SIZE);
		f.setDirty();
		// garbage collect page object
		p = null;
		try {
			buf.flushFileHeader();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Creates the page header
	 * 
	 * @param pageID
	 * @throws IOException
	 */
	public void createPageHeader(int pageID) throws IOException {
		buf.newPage(pageID);
		System.out.println("---Inside the createPageHeader---");
		// System.out
		// .println("The last allocated pageID after newPage execution is "
		// + DiskManager.last_allocated_pageID);
		System.out.println("---Inside the createPageHeader---");
		Frame f = buf.allocFrame(pageID);
		Page p = new Page(pageID, f.getBufferFromFrame());
		p.writeInt(OFFSET_CURRENT_PAGE, pageID);
		// p.writeInt(NEXT_PAGE_OFFSET, (DiskManager.last_allocated_pageID));
		// p.writeInt(PREVIOUS_PAGE_OFFSET, DiskManager.last_allocated_pageID -
		// 1);
		p.writeInt(OFFSET_NEXT_FREE_SLOT, 12);
		p.writeInt(OFFSET_CURRENT_NUMBER_OF_SLOTS, 0);
		f.setDirty();
		// garbage collect page object
		p = null;
		try {
			buf.flushPage(pageID);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public ByteBuffer getHeaderBuffer() {
		return null;
	}

	/**
	 * Insert a Record<K, V> to the file. It dynamically creates new pages if
	 * the file does not have them and modify appropriately the file and header
	 * pages if necessary.
	 * 
	 * @param record
	 * @throws IOException
	 */
	public <V, K extends Comparable<K>> void insert(Record<K, V> record)
			throws IOException {
		// need to add check logic if the record already exists TODO
		// where is commit? TODO
		int pageID;
		int pageCount;
		int i;
		boolean status = true;
		int key = (Integer) record.getKey();
		int value = (Integer) record.getValue();
		Frame f = buf.allocFrame(0);
		Page header = new Page(0, f.getBufferFromFrame());
		pageCount = header.readInt(OFFSET_PAGES_COUNT);
		System.out.println("THE PAGE COUNT IS " + pageCount);
		for (pageID = 0; pageID < pageCount; pageID++) {
			i = header.readInt((pageID * ENTRY_SIZE) + FILEHEADER_LENGTH);
			System.out.println("THE i IS " + i);
			if (i == 1) {
				status = false;
				break;
			}
		}
		System.out.println("THE PAGEID IS " + pageID);
		// if there is no free space allocate a new page
		if (status) {
			// pageID = pageID +1;
			createPageHeader(pageCount + 1);
			header.writeInt(OFFSET_PAGES_COUNT, (pageCount + 1));
			System.out.println("THE OFFSET_PAGES_COUNT AFTER UPDATE IS "
				+ header.readInt(OFFSET_PAGES_COUNT));
			System.out.println("THE PAGE COUNT IS AFTER INCREASE"
				+ (pageCount + 1));
			header.writeInt(((pageCount) * ENTRY_SIZE) + FILEHEADER_LENGTH, 1);
			System.out.println("THE POSITION IS "
				+ (((pageCount) * ENTRY_SIZE) + FILEHEADER_LENGTH));
			// this might be outside
			buf.flushFileHeader();
		}
		// changing the pageID so that it follows the pageID=0 in loop is page 1
		pageID = pageID + 1;
		System.out.println("THE PAGEID IS before Frame in " + pageID);
		Frame in = buf.allocFrame(pageID);
		Page p = new Page(pageID, in.getBufferFromFrame());
		int freeSlot = p.readInt(OFFSET_NEXT_FREE_SLOT);
		int current_number_of_slots = p.readInt(OFFSET_CURRENT_NUMBER_OF_SLOTS);
		System.out.println("THE freeSlot is " + freeSlot);
		System.out.println("THE current number of slot is "
			+ current_number_of_slots);
		p.writeInt(freeSlot, key);
		p.writeInt(freeSlot + 4, value);
		// update the page header
		p.writeInt(OFFSET_NEXT_FREE_SLOT, freeSlot + RECORD_SIZE);
		p.writeInt(OFFSET_CURRENT_NUMBER_OF_SLOTS, current_number_of_slots + 1);
		// prints the content of the buffer for the page
		System.out.println("THE key is " + p.readInt(freeSlot));
		System.out.println("THE value is " + p.readInt(freeSlot + 4));
		System.out.println("THE offset is " + p.readInt(OFFSET_NEXT_FREE_SLOT));
		System.out.println("THE number of slots are "
			+ p.readInt(OFFSET_CURRENT_NUMBER_OF_SLOTS));
		// update the page header
		if (current_number_of_slots + 1 == MAXIMUM_NUMBER_OF_SLOTS) {
			int curr_id = p.readInt(OFFSET_CURRENT_PAGE) - 1;
			System.out.println("The curr_id is " + curr_id);
			header.writeInt((curr_id * ENTRY_SIZE) + FILEHEADER_LENGTH, -1);
			// this might be outside
			buf.flushFileHeader();
		}
		// sets the frame dirty
		in.setDirty();
		// we do not need anymore the pages objects
		header = null;
		p = null;
		try {
			buf.flushPage(pageID);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * public void insert(int key, int value) throws IOException { Frame f =
	 * buf.allocFrame(0); Page header = new Page(0, f.getBufferFromFrame()); int
	 * pageID = header.readInt(OFFSET_FREE_LIST); Frame in =
	 * buf.allocFrame(pageID); Page p = new Page(pageID,
	 * in.getBufferFromFrame()); p.writeInt(OFFSET_DATA_START, key);
	 * p.writeInt(OFFSET_DATA_START + 4, value); in.setDirty(); header = null; p
	 * = null; try { buf.flushPage(pageID); } catch (IOException e) {
	 * e.printStackTrace(); } }
	 */
	public static void main(String args[]) throws IOException {
		HF heapfile = new HF();
		heapfile.createFileHeader();
		heapfile.createPageHeader(1);
		heapfile.createPageHeader(2);
		/*
		 * System.out.println("The pinning occurs here"); Frame f =
		 * buf.allocFrame(0); Page header = new Page(0, f.getBufferFromFrame());
		 * ByteBuffer b = header.getData(); for (int i = 0; i < 5; i = i + 4) {
		 * System.out.println(" "); System.out.println("The values is " +
		 * b.getInt(i)); System.out.println(" "); }
		 * System.out.println("The values is " + b.getShort(8));
		 * System.out.println("The numframe of the header is " +
		 * f.getFrameNumber()); System.out.println("Is it empty?  " +
		 * f.isEmpty());
		 */
	}
}
