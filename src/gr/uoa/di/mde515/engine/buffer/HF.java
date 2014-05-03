package gr.uoa.di.mde515.engine.buffer;

import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.index.DataFile;
import gr.uoa.di.mde515.index.Record;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class HF<K extends Comparable<K>, V> extends DataFile<K, V> {

	private BufferManager buf;
	// storage layer
	private final DiskFile file;
	// useful constants
	private static final int PAGE_SIZE = 48; // TODO move to globals
	private static final int RECORD_SIZE = 8; // TODO read from header
	private int last_allocated_page = 0;
	// it the size of entry in the fileheader
	private static final int ENTRY_SIZE = 4;
	private static final int FILEHEADER_LENGTH = 14;
	// new file header
	private static final int OFFSET_FREE_LIST = 0;
	private static final int OFFSET_FULL_LIST = 4;
	private static final int OFFSET_LAST_FREE_HEADER = 8;
	private static final int OFFSET_RECORD_SIZE = 12;
	// PAGE HEADER OFFSETS
	// Instead of linked list heap file, I changed to directory of pages
	// maximum # of slots in page
	private static final int MAXIMUM_NUMBER_OF_SLOTS = 3;
	private static final int PAGE_FILE_HEADER_LENGTH = 20;
	private static final int OFFSET_CURRENT_PAGE = 0;
	private static final int OFFSET_NEXT_FREE_SLOT = 4;
	private static final int OFFSET_CURRENT_NUMBER_OF_SLOTS = 8;
	private static final int OFFSET_NEXT_PAGE = 12;
	private static final int OFFSET_PREVIOUS_PAGE = 16;

	public HF(String filename) {
		try {
			file = new DiskFile(filename);
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Can't access db file", e);
		}
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
		file.allocateNewPage(0);
		Frame f = buf.allocFrame(0, file);
		Page p = new Page(0, f.getBufferFromFrame());
		p.writeInt(OFFSET_FREE_LIST, -1);
		p.writeInt(OFFSET_FULL_LIST, -1);
		p.writeInt(OFFSET_LAST_FREE_HEADER, -1);
		p.writeShort(OFFSET_RECORD_SIZE, (short) RECORD_SIZE);
		f.setDirty();
		buf.flushFileHeader(file);
	}

	/**
	 * Creates the page header
	 *
	 * @param pageID
	 * @throws IOException
	 */
	public void createPageHeader(int pageID) throws IOException {
		file.allocateNewPage(pageID);
		Frame f = buf.allocFrame(pageID, file);
		Page p = new Page(pageID, f.getBufferFromFrame());
		p.writeInt(OFFSET_CURRENT_PAGE, pageID);
		p.writeInt(OFFSET_NEXT_FREE_SLOT, PAGE_FILE_HEADER_LENGTH);
		p.writeInt(OFFSET_CURRENT_NUMBER_OF_SLOTS, 0);
		p.writeInt(OFFSET_NEXT_PAGE, -1);
		p.writeInt(OFFSET_PREVIOUS_PAGE, 0);
		f.setDirty();
		try {
			buf.flushPage(pageID, file);
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
	@Override
	public void insert(Transaction tr, Record<K, V> record) throws IOException {
		int key = (Integer) record.getKey();
		int value = (Integer) record.getValue();
		Frame f = buf.allocFrame(0, file);
		Page header = new Page(0, f.getBufferFromFrame());
		// if ((header.readInt(OFFSET_CURRENT_PAGE)))
		if (header.readInt(OFFSET_FREE_LIST) == -1) {
			createPageHeader(last_allocated_page + 1);
			header.writeInt(OFFSET_FREE_LIST, last_allocated_page + 1);
			buf.flushFileHeader(file);
			last_allocated_page++;
		}
		int pageID = header.readInt(OFFSET_FREE_LIST);
		System.out.println("The pageID is " + pageID);
		Frame in = buf.allocFrame(pageID, file);
		Page p = new Page(pageID, in.getBufferFromFrame());
		int freeSlot = p.readInt(OFFSET_NEXT_FREE_SLOT);
		int current_number_of_slots = p.readInt(OFFSET_CURRENT_NUMBER_OF_SLOTS);
		System.out.println("The freeSlot is " + freeSlot);
		p.writeInt(freeSlot, key);
		p.writeInt(freeSlot + 4, value);
		p.writeInt(OFFSET_NEXT_FREE_SLOT, freeSlot + RECORD_SIZE);
		p.writeInt(OFFSET_CURRENT_NUMBER_OF_SLOTS, current_number_of_slots + 1);
		if (current_number_of_slots + 1 == MAXIMUM_NUMBER_OF_SLOTS) {
			int next_page = p.readInt(OFFSET_NEXT_PAGE);
			header.writeInt(OFFSET_FREE_LIST, next_page);
			if (next_page != -1) {
				Frame sec = buf.allocFrame(next_page, file);
				Page s = new Page(next_page, sec.getBufferFromFrame());
				s.writeInt(OFFSET_PREVIOUS_PAGE, 0);
				s.setDirty();
				buf.flushPage(next_page, file);
			}
		}
		// sets the frame dirty
		in.setDirty();
		// we do not need anymore the pages objects
		header = null;
		p = null;
		try {
			buf.flushPage(pageID, file);
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
	public static void main1(String args[]) throws IOException {
		HF heapfile = new HF("test.db");
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

	@Override
	public void close() throws IOException {
		file.close();
	}
}
