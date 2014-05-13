package gr.uoa.di.mde515.files;

import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.engine.buffer.BufferManager;
import gr.uoa.di.mde515.engine.buffer.Page;
import gr.uoa.di.mde515.index.PageId;
import gr.uoa.di.mde515.index.Record;
import gr.uoa.di.mde515.locks.DBLock;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

public final class HeapFile<K extends Comparable<K>, V> extends DataFile<K, V> {

	private static final BufferManager<Integer> buf = BufferManager
		.getInstance();
	// storage layer
	private final DiskFile file;
	private final Header header;
	// useful constants
	private static final int PAGE_SIZE = 48; // TODO move to globals
	// it the size of entry in the fileheader
	private static final int ENTRY_SIZE = 4;
	private static final int PAGE_FILE_HEADER_LENGTH = 20;
	private static final int UNDEFINED = -1;
	// PAGE HEADER OFFSETS
	private static final int OFFSET_CURRENT_PAGE = 0;
	private static final int OFFSET_NEXT_FREE_SLOT = 4;
	private static final int OFFSET_CURRENT_NUMBER_OF_SLOTS = 8;
	private static final int OFFSET_NEXT_PAGE = 12;
	private static final int OFFSET_PREVIOUS_PAGE = 16;

	public HeapFile(String filename, short recordSize) throws IOException,
			InterruptedException {
		try {
			file = new DiskFile(filename);
			header = new Header(file, recordSize);
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Can't access db file", e);
		}
	}

	private final static class Header {

		private int freeList = UNDEFINED;
		private int fullList = UNDEFINED;
		private int numOfPages;
		private final Page<Integer> header_page;
		private final int RECORD_SIZE;
		private final double MAXIMUM_NUMBER_OF_SLOTS;
		// OFFSETS
		private static final int OFFSET_FREE_LIST = 0;
		private static final int OFFSET_FULL_LIST = 4;
		private static final int OFFSET_LAST_FREE_HEADER = 8;
		private static final int OFFSET_RECORD_SIZE = 12;
		private static final int OFFSET_NUM_OF_PAGES = 14;

		Header(DiskFile file, short recordSize) throws IOException,
				InterruptedException {
			if (file.read() != -1) {
				System.out.println("File already exists"); // FIXME read header
				header_page = buf.allocPinPage(0, file);
				freeList = header_page.readInt(OFFSET_FREE_LIST);
				fullList = header_page.readInt(OFFSET_FULL_LIST);
				RECORD_SIZE = header_page.readShort(OFFSET_RECORD_SIZE);
				numOfPages = header_page.readInt(OFFSET_NUM_OF_PAGES);
			} else { // FILE EMPTY - CREATE THE HEADER
				System.out.println("Creating the file");
				header_page = buf.allocFrameForNewPage(0);
				header_page.writeInt(OFFSET_FREE_LIST, UNDEFINED);
				header_page.writeInt(OFFSET_FULL_LIST, UNDEFINED);
				header_page.writeInt(OFFSET_LAST_FREE_HEADER, UNDEFINED);
				header_page.writeShort(OFFSET_RECORD_SIZE, recordSize);
				header_page.writeInt(OFFSET_NUM_OF_PAGES, 0);
				RECORD_SIZE = recordSize;
				buf.setPageDirty(0);
				buf.flushPage(0, file);
			}
			MAXIMUM_NUMBER_OF_SLOTS = Math
				.floor((PAGE_SIZE - PAGE_FILE_HEADER_LENGTH) / RECORD_SIZE);
		}

		int getFreeList() {
			return freeList;
		}

		void setFreeList(int freepageID) {
			freeList = freepageID;
		}

		private void pageWriteFreeList(int freelist) {
			header_page.writeInt(OFFSET_FREE_LIST, freelist);
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("Header [freeList=");
			builder.append(freeList);
			builder.append("]");
			return builder.toString();
		}

		void pageWrite() {
			pageWriteFreeList(freeList);
			pageWriteNumOfPages(numOfPages);
		}

		int getNumOfPages() {
			return numOfPages;
		}

		void setNumOfPages(int num) {
			numOfPages = num;
		}

		void pageWriteNumOfPages(int numOfPages) {
			header_page.writeInt(OFFSET_NUM_OF_PAGES, numOfPages);
		}
	}

	public BufferManager<Integer> buf() {
		return buf;
	}

	/**
	 * Creates the page header.
	 *
	 * @param pageID
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void createPageInMemory(int pageID) throws IOException,
			InterruptedException {
		Page<?> p = buf.allocFrameForNewPage(pageID);
		p.writeInt(OFFSET_CURRENT_PAGE, pageID);
		p.writeInt(OFFSET_NEXT_FREE_SLOT, PAGE_FILE_HEADER_LENGTH);
		p.writeInt(OFFSET_CURRENT_NUMBER_OF_SLOTS, 0);
		p.writeInt(OFFSET_NEXT_PAGE, UNDEFINED);
		p.writeInt(OFFSET_PREVIOUS_PAGE, 0);
		buf.setPageDirty(pageID);
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
		int pageID = getFreeListPageId();
		// PageId<Integer> p = nextAvailablePage();
		// Request<Integer> request = new LockManager.Request<>(p, tr, Lock.E);
		// lock manager request
		Page<Integer> p;
		if (tr.lock(pageID, DBLock.E)) {
			p = buf.allocFrame(pageID, file);
			pinPage(pageID);
		} else {
			p = buf.getAssociatedFrame(pageID);
		}
		int current_number_of_slots = p.readInt(OFFSET_CURRENT_NUMBER_OF_SLOTS);
		writeIntoFrame(p, (Integer) record.getKey(),
			(Integer) record.getValue(), current_number_of_slots);
		checkReachLimitOfPage(p, current_number_of_slots);
	}

	@Override
	public void flush(List<PageId<Integer>> pageIds) throws IOException {
		header.pageWrite();
		for (PageId<Integer> pageID : pageIds) {
			final Integer pid = pageID.getId();
			buf.flushPage(pid, file);
			System.out.println("PID " + pid);
			buf.unpinPage(pid);
		}
	}

	@Override
	public void close() throws IOException {
		file.close();
	}

	@Override
	public void lockHeader(Transaction tr, DBLock e) {
		tr.lock(0, e); // FIXME pin
	}

	@Override
	public void pinPage(int pageId) {
		buf.pinPage(pageId);
	}

	// =========================================================================
	// Helpers
	// =========================================================================
	private int getFreeListPageId() throws IOException, InterruptedException {
		if (header.getFreeList() == UNDEFINED) {
			createPageInMemory(header.getNumOfPages() + 1);
			header.setFreeList(header.getNumOfPages() + 1);
			header.setNumOfPages(header.getNumOfPages() + 1);
			buf.setPageDirty(0);
		}
		int pageID = header.getFreeList();
		System.out.println("PageId " + pageID);
		return pageID;
	}

	private void writeIntoFrame(Page<Integer> p, int key, int value,
			int current_number_of_slots) { // FIXME STRING
		int freeSlot = p.readInt(OFFSET_NEXT_FREE_SLOT);
		System.out.println("The freeSlot is " + freeSlot);
		p.writeInt(freeSlot, key);
		p.writeInt(freeSlot + ENTRY_SIZE, value);
		p.writeInt(OFFSET_NEXT_FREE_SLOT, freeSlot + header.RECORD_SIZE);
		p.writeInt(OFFSET_CURRENT_NUMBER_OF_SLOTS, current_number_of_slots + 1);
		buf.setPageDirty(p.getPageId().getId());
	}

	private void checkReachLimitOfPage(Page<Integer> p,
			int current_number_of_slots) throws IOException,
			InterruptedException {
		if (current_number_of_slots + 1 == header.MAXIMUM_NUMBER_OF_SLOTS) {
			int next_page = p.readInt(OFFSET_NEXT_PAGE);
			header.setFreeList(next_page);
			if (next_page != UNDEFINED) {
				Page<Integer> s = buf.allocFrame(next_page, file);
				s.writeInt(OFFSET_PREVIOUS_PAGE, 0);
				buf.setPageDirty(s.getPageId().getId());
				buf.flushPage(next_page, file);
			}
		}
	}
}
