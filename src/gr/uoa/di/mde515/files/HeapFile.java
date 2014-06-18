package gr.uoa.di.mde515.files;

import gr.uoa.di.mde515.engine.Engine;
import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.engine.buffer.BufferManager;
import gr.uoa.di.mde515.engine.buffer.Page;
import gr.uoa.di.mde515.engine.buffer.RecordsPage;
import gr.uoa.di.mde515.engine.buffer.Serializer;
import gr.uoa.di.mde515.index.Record;
import gr.uoa.di.mde515.locks.DBLock;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

public final class HeapFile<K extends Comparable<K>, V> extends DataFile<K, V> {

	private static final BufferManager buf = BufferManager.getInstance();
	// storage layer
	private final DiskFile file;
	private final Header head;
	// useful constants
	private static final int PAGE_SIZE = Engine.PAGE_SIZE;
	// it the size of entry in the fileheader
	private static final int KEY_SIZE = 4;
	private static final short PAGE_HEADER_LENGTH = 20; // TODO move to page
	private static final int UNDEFINED = -1;
	// PAGE HEADER OFFSETS
	private static final int OFFSET_CURRENT_PAGE = 0;
	private static final int OFFSET_NEXT_FREE_SLOT = 4;
	private static final int OFFSET_CURRENT_NUMBER_OF_SLOTS = 8;
	private static final int OFFSET_NEXT_PAGE = 12;
	private static final int OFFSET_PREVIOUS_PAGE = 16;
	// Used to write K and V to disc and read them back
	private final Serializer<K> serKey;
	private final Serializer<V> serVal;

	public HeapFile(String filename, Serializer<K> serKey, Serializer<V> serVal)
			throws IOException, InterruptedException {
		this.serKey = serKey;
		this.serVal = serVal;
		try {
			file = new DiskFile(filename);
			head = new Header();
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Can't access db file", e);
		}
	}

	private final class Header extends Page {

		final short RECORD_SIZE;
		final short MAXIMUM_NUMBER_OF_SLOTS;
		// state fields
		private int freeList = UNDEFINED;
		private int numOfPages;
		// OFFSETS
		private static final int OFFSET_FREE_LIST = 0;
		private static final int OFFSET_RECORD_SIZE = 12;
		private static final int OFFSET_NUM_OF_PAGES = 14;

		@SuppressWarnings("synthetic-access")
		Header() throws IOException, InterruptedException {
			super(buf.allocPermanentPage(0, file));
			if (file.read() != -1) {
				System.out.println("File already exists");
				freeList = readInt(OFFSET_FREE_LIST);
				RECORD_SIZE = readShort(OFFSET_RECORD_SIZE);
				numOfPages = readInt(OFFSET_NUM_OF_PAGES);
			} else { // FILE EMPTY - CREATE THE HEADER
				System.out.println("Creating the file");
				RECORD_SIZE = (short) (serKey.getTypeSize() + serVal
					.getTypeSize());
				writeInt(OFFSET_FREE_LIST, UNDEFINED);
				writeShort(OFFSET_RECORD_SIZE, RECORD_SIZE);
				writeInt(OFFSET_NUM_OF_PAGES, 0);
				buf.flushPage(0, file); // TODO - watch out: wild flush
			}
			MAXIMUM_NUMBER_OF_SLOTS = (short) ((PAGE_SIZE - PAGE_HEADER_LENGTH) / RECORD_SIZE);
			if (MAXIMUM_NUMBER_OF_SLOTS < 1)
				throw new AssertionError("Page too small");
		}

		void pageWrite() {
			pageWriteFreeList(freeList);
			pageWriteNumOfPages(numOfPages);
		}

		// =====================================================================
		// Accessors/Mutators
		// =====================================================================
		// FIXME sync
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
			writeInt(OFFSET_NUM_OF_PAGES, numPages);
		}

		private void pageWriteFreeList(int freelist) {
			writeInt(OFFSET_FREE_LIST, freelist);
		}
	}

	private final class HeapPage extends RecordsPage<K, V> {

		public HeapPage(Page page) {
			super(page, serKey, serVal, PAGE_HEADER_LENGTH);
		}

		@Override
		@SuppressWarnings("synthetic-access")
		protected short getMaxKeys() {
			return head.MAXIMUM_NUMBER_OF_SLOTS;
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
	public int insert(Transaction tr, Record<K, V> record) throws IOException,
			InterruptedException {
		int pageID = getFreeListPageId();
		HeapPage p = new HeapPage(alloc(tr, pageID));
		writeIntoFrame(p, (Integer) record.getKey(),
			(Integer) record.getValue());
		checkReachLimitOfPage(p, tr);
		return pageID;
	}

	@Override
	public void delete(Transaction tr, int pid, K key) throws IOException,
			InterruptedException {
		int newFreeSlotposition = 0;
		Page deleteFromPage = alloc(tr, pid);
		for (int i = PAGE_HEADER_LENGTH, j = 0; j < head.MAXIMUM_NUMBER_OF_SLOTS; i += head.RECORD_SIZE, ++j) {
			if (key.compareTo((K) (Integer) deleteFromPage.readInt(i)) == 0) {
				newFreeSlotposition = i;
			}
			// else FIXME
		}
		int used_slots = deleteFromPage.readInt(OFFSET_CURRENT_NUMBER_OF_SLOTS);
		int old_free_slot_position = deleteFromPage
			.readInt(OFFSET_NEXT_FREE_SLOT);
		if (used_slots != head.MAXIMUM_NUMBER_OF_SLOTS) {
			if (newFreeSlotposition > old_free_slot_position) {
				for (int i = 1; i < (head.MAXIMUM_NUMBER_OF_SLOTS - used_slots); i++) {
					old_free_slot_position = deleteFromPage
						.readInt(old_free_slot_position + KEY_SIZE);
				}
				// delete the key and value needs no update of the page header
				deleteFromPage.writeInt(old_free_slot_position + KEY_SIZE,
					newFreeSlotposition);
				deleteFromPage.writeInt(newFreeSlotposition, UNDEFINED);
				deleteFromPage.writeInt(newFreeSlotposition + KEY_SIZE, 0);
				deleteFromPage.writeInt(OFFSET_CURRENT_NUMBER_OF_SLOTS,
					used_slots - 1);
			} else {
				deleteFromPage.writeInt(newFreeSlotposition, UNDEFINED);
				deleteFromPage.writeInt(newFreeSlotposition + KEY_SIZE,
					old_free_slot_position);
				// update the page header
				deleteFromPage.writeInt(OFFSET_NEXT_FREE_SLOT,
					newFreeSlotposition);
				deleteFromPage.writeInt(OFFSET_CURRENT_NUMBER_OF_SLOTS,
					used_slots - 1);
			}
		} else {
			deleteFromPage.writeInt(newFreeSlotposition, UNDEFINED);
			deleteFromPage.writeInt(newFreeSlotposition + KEY_SIZE, 0);
			// update the page header
			deleteFromPage.writeInt(OFFSET_NEXT_FREE_SLOT, newFreeSlotposition);
			deleteFromPage.writeInt(OFFSET_CURRENT_NUMBER_OF_SLOTS,
				used_slots - 1);
			// update the file header
			if (head.getFreeList() != UNDEFINED) {
				// get next free page
				Page nextPage = alloc(tr, head.getFreeList());
				nextPage.writeInt(OFFSET_PREVIOUS_PAGE,
					deleteFromPage.readInt(OFFSET_CURRENT_PAGE));
				deleteFromPage.writeInt(OFFSET_NEXT_PAGE,
					nextPage.readInt(OFFSET_CURRENT_PAGE));
				deleteFromPage.writeInt(OFFSET_PREVIOUS_PAGE, 0);
				head.setFreeList(deleteFromPage.readInt(OFFSET_CURRENT_PAGE));
				head.pageWrite();
			} else {
				head.setFreeList(deleteFromPage.readInt(OFFSET_CURRENT_PAGE));
				deleteFromPage.writeInt(OFFSET_NEXT_PAGE, UNDEFINED);
				deleteFromPage.writeInt(OFFSET_PREVIOUS_PAGE, 0);
				head.pageWrite();
			}
		}
	}

	@Override
	public V get(Transaction tr, int pid, K key) throws IOException,
			InterruptedException {
		if (key == null)
			throw new NullPointerException("Trying to get a null key");
		HeapPage allocFrame = new HeapPage(buf.allocFrame(pid, file));
		return allocFrame._get(key);
	}

	@Override
	public void abort(List<Integer> pageIds) throws IOException {
		for (int pageID : pageIds) {
			buf.killPage(pageID, file);
		}
	}

	@Override
	public void flush(List<Integer> pageIds) throws IOException {
		head.pageWrite();
		for (int pageID : pageIds) {
			buf.flushPage(pageID, file);
			System.out.println("PID " + pageID);
			buf.unpinPage(pageID);
		}
	}

	@Override
	public void close() throws IOException {
		file.close();
	}

	@Override
	public void lockHeader(Transaction tr, DBLock e)
			throws InterruptedException {
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
		Page p = buf.allocFrameForNewPage(pageID);
		for (int i = 0; i < PAGE_SIZE; i = i + 4) {
			p.writeInt(i, 0);
		}
		p.writeInt(OFFSET_CURRENT_PAGE, pageID);
		p.writeInt(OFFSET_NEXT_FREE_SLOT, PAGE_HEADER_LENGTH);
		p.writeInt(OFFSET_CURRENT_NUMBER_OF_SLOTS, 0);
		p.writeInt(OFFSET_NEXT_PAGE, UNDEFINED);
		p.writeInt(OFFSET_PREVIOUS_PAGE, 0);
	}

	private void writeIntoFrame(Page p, int key, int value) {
		int freeSlot = p.readInt(OFFSET_NEXT_FREE_SLOT);
		// consider the free slots
		int nextFreeSlot = p.readInt(freeSlot + KEY_SIZE); // OUT OF BOUNDS
		if (nextFreeSlot == 0) {
			p.writeInt(freeSlot, key);
			p.writeInt(freeSlot + KEY_SIZE, value);
			p.writeInt(OFFSET_NEXT_FREE_SLOT, freeSlot + head.RECORD_SIZE);
			int current_number_of_slots = p
				.readInt(OFFSET_CURRENT_NUMBER_OF_SLOTS);
			p.writeInt(OFFSET_CURRENT_NUMBER_OF_SLOTS,
				current_number_of_slots + 1);
		} else {
			// update the next slot
			p.writeInt(OFFSET_NEXT_FREE_SLOT, nextFreeSlot);
			// write the record
			p.writeInt(freeSlot, key);
			p.writeInt(freeSlot + KEY_SIZE, value);
			int current_number_of_slots = p
				.readInt(OFFSET_CURRENT_NUMBER_OF_SLOTS);
			p.writeInt(OFFSET_CURRENT_NUMBER_OF_SLOTS,
				current_number_of_slots + 1);
		}
	}

	private void checkReachLimitOfPage(Page page, Transaction tr)
			throws IOException, InterruptedException {
		int current_number_of_slots = page
			.readInt(OFFSET_CURRENT_NUMBER_OF_SLOTS);
		if (current_number_of_slots == head.MAXIMUM_NUMBER_OF_SLOTS) {
			int next_page = page.readInt(OFFSET_NEXT_PAGE);
			System.out.println("The next header is " + next_page);
			page.writeInt(OFFSET_NEXT_PAGE, UNDEFINED);
			page.writeInt(OFFSET_PREVIOUS_PAGE, 0);
			head.setFreeList(next_page);
			if (next_page != UNDEFINED) {
				System.out.println("The NEXT FRAME HERE");
				Page p = alloc(tr, next_page);
				p.writeInt(OFFSET_PREVIOUS_PAGE, 0);
				// buf.flushPage(next_page, file); // FIXME FLUSH ??
			}
		}
	}

	private Page alloc(Transaction tr, int pageID) throws IOException,
			InterruptedException {
		Page p;
		// TODO DBLock.E parameter
		if (tr.lock(pageID, DBLock.E)) { // locks for the first time
			p = buf.allocFrame(pageID, file);
			// FIXME - race in pin ??? - add boolean pin param in allocFrame
			buf.pinPage(pageID);
		} else {
			p = buf.allocFrame(pageID, file);
		}
		return p;
	}
}
