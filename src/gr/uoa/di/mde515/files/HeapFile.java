package gr.uoa.di.mde515.files;

import gr.uoa.di.mde515.engine.Engine;
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
	private final Header head;
	// useful constants
	private static final int PAGE_SIZE = Engine.PAGE_SIZE;
	// it the size of entry in the fileheader
	private static final int KEY_SIZE = 4;
	private static final int PAGE_HEADER_LENGTH = 20; // TODO move to header
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
			head = new Header(file, recordSize);
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Can't access db file", e);
		}
	}

	private final static class Header {

		final int RECORD_SIZE;
		final short MAXIMUM_NUMBER_OF_SLOTS;
		// state fields
		private int freeList = UNDEFINED;
		private int fullList = UNDEFINED;
		private int numOfPages;
		// OFFSETS
		private static final int OFFSET_FREE_LIST = 0;
		private static final int OFFSET_FULL_LIST = 4;
		private static final int OFFSET_LAST_FREE_HEADER = 8;
		private static final int OFFSET_RECORD_SIZE = 12;
		private static final int OFFSET_NUM_OF_PAGES = 14;
		// BufferManager
		private static final BufferManager<Integer> buff = BufferManager
			.getInstance();
		private final Page<Integer> header_page;

		Header(DiskFile file, short recordSize) throws IOException,
				InterruptedException {
			if (file.read() != -1) {
				System.out.println("File already exists"); // FIXME read header
				header_page = buff.allocPermanentPage(0, file);
				freeList = header_page.readInt(OFFSET_FREE_LIST);
				fullList = header_page.readInt(OFFSET_FULL_LIST);
				RECORD_SIZE = header_page.readShort(OFFSET_RECORD_SIZE);
				numOfPages = header_page.readInt(OFFSET_NUM_OF_PAGES);
			} else { // FILE EMPTY - CREATE THE HEADER
				System.out.println("Creating the file");
				header_page = buff.allocPermanentPage(0, file);
				header_page.writeInt(OFFSET_FREE_LIST, UNDEFINED);
				header_page.writeInt(OFFSET_FULL_LIST, UNDEFINED);
				header_page.writeInt(OFFSET_LAST_FREE_HEADER, UNDEFINED);
				header_page.writeShort(OFFSET_RECORD_SIZE, recordSize);
				header_page.writeInt(OFFSET_NUM_OF_PAGES, 0);
				RECORD_SIZE = recordSize;
				buff.setPageDirty(0);
				buff.flushPage(0, file); // TODO - watch out: wild flush
			}
			MAXIMUM_NUMBER_OF_SLOTS = (short) ((PAGE_SIZE - PAGE_HEADER_LENGTH) / RECORD_SIZE);
			if (MAXIMUM_NUMBER_OF_SLOTS < 1)
				throw new AssertionError("TODO error checking");
		}

		void pageWrite() {
			pageWriteFreeList(freeList);
			pageWriteNumOfPages(numOfPages);
			buff.setPageDirty(0);
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("Header [freeList=");
			builder.append(freeList);
			builder.append("]");
			return builder.toString();
		}

		// =====================================================================
		// Accessors/Mutators
		// =====================================================================
		// FIXME sync KLEO
		int getNumOfPages() {
			return numOfPages;
		}

		void setNumOfPages(int num) {
			numOfPages = num;
		}

		int getFreeList() {
			return freeList;
		}

		void setFreeList(int freepageID) {
			freeList = freepageID;
		}

		// =====================================================================
		// Helpers
		// =====================================================================
		private void pageWriteNumOfPages(int numPages) {
			header_page.writeInt(OFFSET_NUM_OF_PAGES, numPages);
		}

		private void pageWriteFreeList(int freelist) {
			header_page.writeInt(OFFSET_FREE_LIST, freelist);
		}
	}

	// =========================================================================
	// API
	// =========================================================================
	/**
	 * Insert a Record<K, V> to the file. It dynamically creates new pages if
	 * the file does not have them and modify appropriately the file and header
	 * pages if necessary. New pages are created on insert which means that head
	 * is locked for writing therefore there is no race in locking the newly
	 * created page. The header of the file should be locked beforehand for
	 * writing. Blocks if the buffer manager has no available frames.
	 *
	 * @param record
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public PageId insert(Transaction tr, Record<K, V> record)
			throws IOException, InterruptedException {
		int pageID = getFreeListPageId();
		Page<Integer> p;
		if (tr.lock(pageID, DBLock.E)) { // locks for the first time
			p = buf.allocFrame(pageID, file);
			// FIXME - race in pin ??? - add boolean pin param in allocFrame
			buf.pinPage(pageID);
		} else {
			p = buf.allocFrame(pageID, file);
		}
		writeIntoFrame(p, (Integer) record.getKey(),
			(Integer) record.getValue());
		checkReachLimitOfPage(p);
		return new PageId(pageID);
	}

	@Override
	public <T> V get(Transaction tr, PageId<T> p, K key) throws IOException,
			InterruptedException {
		if(key==null)throw new NullPointerException("Trying to get a null key");
		Page<Integer> allocFrame = buf.allocFrame((Integer) p.getId(), file);
		for (int i = PAGE_HEADER_LENGTH, j = 0; j < head.MAXIMUM_NUMBER_OF_SLOTS; i += head.RECORD_SIZE, ++j) {
			if (key.compareTo((K) (Integer) allocFrame.readInt(i)) == 0)
				return (V) (Integer) allocFrame.readInt(i + KEY_SIZE);
		}
		return null;
	}

	@Override
	public void abort(List<PageId<Integer>> pageIds) {
		System.out.println("FINALLY REACHED");
		for (PageId<Integer> pageID : pageIds) {
			buf.killPage(pageID.getId());
		}
	}

	@Override
	public void flush(List<PageId<Integer>> pageIds) throws IOException {
		head.pageWrite();
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

	// =========================================================================
	// Helpers
	// =========================================================================
	private int getFreeListPageId() throws InterruptedException {
		// TODO maybe sync is excessive - only one writer - still memory model..
		synchronized (head) {
			final int freeList = head.getFreeList();
			if (freeList == UNDEFINED) {
				final int pageID = head.getNumOfPages() + 1;
				createPageInMemory(pageID);
				head.setFreeList(pageID);
				head.setNumOfPages(pageID);
				buf.setPageDirty(0);
				return pageID;
			}
			return freeList;
		}
	}

	/**
	 * Allocates a frame in memory for a new page and writes its header.
	 *
	 * @param pageID
	 * @throws InterruptedException
	 */
	private static void createPageInMemory(int pageID)
			throws InterruptedException {
		Page<?> p = buf.allocFrameForNewPage(pageID);
		p.writeInt(OFFSET_CURRENT_PAGE, pageID);
		p.writeInt(OFFSET_NEXT_FREE_SLOT, PAGE_HEADER_LENGTH);
		p.writeInt(OFFSET_CURRENT_NUMBER_OF_SLOTS, 0);
		p.writeInt(OFFSET_NEXT_PAGE, UNDEFINED);
		p.writeInt(OFFSET_PREVIOUS_PAGE, 0);
		buf.setPageDirty(pageID);
	}

	private void writeIntoFrame(Page<Integer> p, int key, int value) {
		int freeSlot = p.readInt(OFFSET_NEXT_FREE_SLOT);
		p.writeInt(freeSlot, key);
		p.writeInt(freeSlot + KEY_SIZE, value);
		p.writeInt(OFFSET_NEXT_FREE_SLOT, freeSlot + head.RECORD_SIZE);
		int current_number_of_slots = p.readInt(OFFSET_CURRENT_NUMBER_OF_SLOTS);
		p.writeInt(OFFSET_CURRENT_NUMBER_OF_SLOTS, current_number_of_slots + 1);
		buf.setPageDirty(p.getPageId().getId());
	}

	private void checkReachLimitOfPage(Page<Integer> p) throws IOException,
			InterruptedException {
		int current_number_of_slots = p.readInt(OFFSET_CURRENT_NUMBER_OF_SLOTS);
		if (current_number_of_slots == head.MAXIMUM_NUMBER_OF_SLOTS) {
			int next_page = p.readInt(OFFSET_NEXT_PAGE);
			head.setFreeList(next_page);
			if (next_page != UNDEFINED) {
				Page<Integer> s = buf.allocFrame(next_page, file);
				s.writeInt(OFFSET_PREVIOUS_PAGE, 0);
				buf.setPageDirty(s.getPageId().getId());
				buf.flushPage(next_page, file); // FIXME FLUSH ??
			}
		}
	}
}
