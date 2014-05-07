package gr.uoa.di.mde515.files;

import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.engine.buffer.BufferManager;
import gr.uoa.di.mde515.engine.buffer.Page;
import gr.uoa.di.mde515.index.PageId;
import gr.uoa.di.mde515.index.Record;
import gr.uoa.di.mde515.locks.DBLock;
import gr.uoa.di.mde515.locks.LockManager;

import java.io.FileNotFoundException;
import java.io.IOException;

public class HeapFile<K extends Comparable<K>, V> extends DataFile<K, V> {

	private static final BufferManager<Integer> buf = BufferManager
		.getInstance();
	// storage layer
	private final DiskFile file;
	// useful constants
	private static final int PAGE_SIZE = 48; // TODO move to globals
	private static final int RECORD_SIZE = 8; // TODO read from header
	private int last_allocated_page = 0;
	// it the size of entry in the fileheader
	private static final int ENTRY_SIZE = 4;
	private static final int PAGE_FILE_HEADER_LENGTH = 20;
	// private final Header head;
	private static final int OFFSET_FREE_LIST = 0;
	private static final int OFFSET_FULL_LIST = 4;
	private static final int OFFSET_LAST_FREE_HEADER = 8;
	private static final int OFFSET_RECORD_SIZE = 12;
	// PAGE HEADER OFFSETS
	private static final int UNDEFINED = -1;
	// maximum # of slots in page
	private static final double MAXIMUM_NUMBER_OF_SLOTS = Math
		.floor((PAGE_SIZE - PAGE_FILE_HEADER_LENGTH) / RECORD_SIZE);
	private static final int OFFSET_CURRENT_PAGE = 0;
	private static final int OFFSET_NEXT_FREE_SLOT = 4;
	private static final int OFFSET_CURRENT_NUMBER_OF_SLOTS = 8;
	private static final int OFFSET_NEXT_PAGE = 12;
	private static final int OFFSET_PREVIOUS_PAGE = 16;
	// LockManager
	private final LockManager lm = LockManager.getInstance();

	public HeapFile(String filename) throws IOException, InterruptedException {
		try {
			file = new DiskFile(filename);
			createFileHeader();
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Can't access db file", e);
		}
		// file = new RandomAccessFile(filename, "rw");
		// KLEO
		// IF file existed:
		// bm.getHeader
		// lm. lockHeader for read (NO TRANSACTION)....
		// read Header and initialize _head_
		// ELSE init a new header and CREATE ONE BLANK PAGE
		// preallocateFile();
	}

	// private final static class Header {
	//
	// private int freeListStart;
	// private int fullListStart;
	// private int numPages;
	// final short RECORD_SIZE;
	//
	// public Header(Page header) {
	// freeListStart = header.readInt(OFFSET_FREE_LIST);
	// fullListStart = header.readInt(OFFSET_FULL_LIST);
	// RECORD_SIZE = header.readShort(OFFSET_RECORD_SIZE);
	// numPages = header.readInt(OFFSET_PAGES_COUNT);
	// try {
	// Path path = Paths.get("/home/temp/", "hugefile.txt");
	// SeekableByteChannel sbc = Files.newByteChannel(path,
	// StandardOpenOption.READ);
	// ByteBuffer bf = ByteBuffer.allocate(941);// line size
	// int i = 0;
	// while ((i = sbc.read(bf)) > 0) {
	// bf.flip();
	// System.out.println(new String(bf.array()));
	// bf.clear();
	// }
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// }
	// // fileheader = file.readPage(0);
	// }
	public BufferManager<Integer> buf() {
		return buf;
	}

	/**
	 * Creates the file header
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void createFileHeader() throws IOException, InterruptedException {
		file.allocateNewPage(0);
		Page<?> p = buf.allocFrame(0, file);
		p.writeInt(OFFSET_FREE_LIST, UNDEFINED);
		p.writeInt(OFFSET_FULL_LIST, UNDEFINED);
		p.writeInt(OFFSET_LAST_FREE_HEADER, UNDEFINED);
		p.writeShort(OFFSET_RECORD_SIZE, (short) RECORD_SIZE);
		buf.setFrameDirty(0);
		buf.flushFileHeader(file);
	}

	/**
	 * Creates the page header
	 *
	 * @param pageID
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void createPageHeader(int pageID) throws IOException,
			InterruptedException {
		file.allocateNewPage(pageID);
		Page<?> p = buf.allocFrame(pageID, file);
		p.writeInt(OFFSET_CURRENT_PAGE, pageID);
		p.writeInt(OFFSET_NEXT_FREE_SLOT, PAGE_FILE_HEADER_LENGTH);
		p.writeInt(OFFSET_CURRENT_NUMBER_OF_SLOTS, 0);
		p.writeInt(OFFSET_NEXT_PAGE, UNDEFINED);
		p.writeInt(OFFSET_PREVIOUS_PAGE, 0);
		buf.setFrameDirty(pageID);
		try {
			buf.flushPage(pageID, file);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Insert a Record<K, V> to the file. It dynamically creates new pages if
	 * the file does not have them and modify appropriately the file and header
	 * pages if necessary. The header of the file should be locked beforehand
	 * for writing. Blocks if the buffer manager has no available frames.
	 *
	 * @param record
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public void insert(Transaction tr, Record<K, V> record) throws IOException,
			InterruptedException {
		Page<Integer> header = buf.allocFrame(0, file); // FIXME block
		int pageID = getFreeListPageId(header);
		System.out.println("The pageID is " + pageID);
		// PageId<Integer> p = nextAvailablePage();
		// Request<Integer> request = new LockManager.Request<>(p, tr, Lock.E);
		// lock manager request
		Page<Integer> p = buf.allocFrame(pageID, file);
		int current_number_of_slots = p.readInt(OFFSET_CURRENT_NUMBER_OF_SLOTS);
		writeIntoFrame(p, (Integer) record.getKey(),
			(Integer) record.getValue(), current_number_of_slots);
		checkReachLimitOfPage(header, p, current_number_of_slots);
		try {
			buf.flushPage(pageID, file);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void commit() {
		throw new UnsupportedOperationException("Not implemented"); // TODO
	}

	@Override
	public void close() throws IOException {
		file.close();
	}

	@Override
	public void lockHeader(Transaction tr, DBLock e) {
		lm.requestLock(new LockManager.Request(new PageId<>(0), tr, e));
	}

	// =========================================================================
	// Helpers
	// =========================================================================
	private int getFreeListPageId(Page<?> header) throws IOException,
			InterruptedException {
		// if ((header.readInt(OFFSET_CURRENT_PAGE)))
		if (header.readInt(OFFSET_FREE_LIST) == UNDEFINED) {
			createPageHeader(last_allocated_page + 1);
			header.writeInt(OFFSET_FREE_LIST, last_allocated_page + 1);
			buf.flushFileHeader(file); // FIXME
			last_allocated_page++;
		}
		int pageID = header.readInt(OFFSET_FREE_LIST);
		return pageID;
	}

	private void writeIntoFrame(Page<Integer> p, int key, int value,
			int current_number_of_slots) { // FIXME STRING
		int freeSlot = p.readInt(OFFSET_NEXT_FREE_SLOT);
		System.out.println("The freeSlot is " + freeSlot);
		p.writeInt(freeSlot, key);
		p.writeInt(freeSlot + ENTRY_SIZE, value);
		p.writeInt(OFFSET_NEXT_FREE_SLOT, freeSlot + RECORD_SIZE);
		p.writeInt(OFFSET_CURRENT_NUMBER_OF_SLOTS, current_number_of_slots + 1);
		buf.setFrameDirty(p.getPageId().getId());
	}

	private void checkReachLimitOfPage(Page<Integer> header, Page<Integer> p,
			int current_number_of_slots) throws IOException,
			InterruptedException {
		if (current_number_of_slots + 1 == MAXIMUM_NUMBER_OF_SLOTS) {
			int next_page = p.readInt(OFFSET_NEXT_PAGE);
			header.writeInt(OFFSET_FREE_LIST, next_page);
			if (next_page != UNDEFINED) {
				Page<Integer> s = buf.allocFrame(next_page, file);
				s.writeInt(OFFSET_PREVIOUS_PAGE, 0);
				buf.setFrameDirty(s.getPageId().getId());
				buf.flushPage(next_page, file);
			}
		}
	}
}
