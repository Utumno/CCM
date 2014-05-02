package gr.uoa.di.mde515.engine.buffer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Kleomenis
 */
public class BufferManager {

	// the pool of frames
	private Frame[] pool;
	// the structure that maintain information about pageIDs and their
	// corresponding frame numbers
	private FramePage hash;
	// contains the available frames that can be used
	private final List<Integer> freeList = new ArrayList<>();

	private BufferManager(int numBufs) {
		// System.out.println("--- Start The BufferManager Constructor---");
		pool = new Frame[numBufs];
		hash = new FramePage();
		for (int i = 0; i < numBufs; i++) {
			pool[i] = new Frame(i);
			// System.out.println("The init hashcode is " + pool[i].hashCode());
			// System.out.println("The init ByteBuffer is "
			// + pool[i].getBufferFromFrame());
			freeList.add(i);
		}
		// System.out.println("---END The BufferManager Constructor---");
	}

	private static final BufferManager instance = new BufferManager(4);

	// =========================================================================
	// API
	// =========================================================================
	/**
	 * The BufferManager follows the singleton pattern. So one BufferManager is
	 * created.
	 */
	public static BufferManager getInstance() {
		return instance;
	}

	public void freePage(/* int frameNumber */) {
		throw new UnsupportedOperationException("Not implemented"); // TODO
	}

	/**
	 * It flushes the content of the frame associated with the given pageID to
	 * the disk at correct block.
	 *
	 * @param pageID
	 * @param disk
	 * @throws IOException
	 */
	public void flushPage(int pageID, DiskFile disk) throws IOException {
		int frameNumber = hash.getValue(pageID);
		// System.out.println("--- Start The flushPage of the BufferManager---");
		// System.out.println("The (flushPage)framenumber is " + frameNumber);
		unpinPage(frameNumber);
		// System.out.println("Is the frame dirty (flusher)? "
		// + pool[frameNumber].isDirty());
		if (pool[frameNumber].isDirty())
			disk.writePage(pageID, pool[frameNumber].getBufferFromFrame());
		cleanPage(frameNumber);
		hash.removeKey(pageID);
		freeList.add(frameNumber);
		_printList();
		// System.out.println("---End The flushPage of the BufferManager---");
	}

	/**
	 * Flushes the header of the file while keeping it pinned in main memory.
	 *
	 * @param disk
	 * @throws IOException
	 */
	public void flushFileHeader(DiskFile disk) throws IOException {
		// if it does not exist?
		disk.writePage(0, pool[0].getBufferFromFrame());
		pool[0].setEmpty();
	}

	/**
	 * It first finds an empty buffer from the free list and then updates the
	 * map of pageIDs and frameNumbers along with returning the specified Frame.
	 *
	 * @param pageID
	 * @param disk
	 * @return ByteBuffer
	 * @throws IOException
	 */
	public Frame allocFrame(int pageID, DiskFile disk) throws IOException {
		if (freeList.isEmpty()) {
			System.out.println("No available buffer"); // FIXME
														// BUfferFullException
		}
		/* if the page already in the buffer return the buffer */
		if (isPage(pageID)) {
			// System.out.println("ENTEREEEEEEEEEEEEEEE");
			pinPage(hash.getValue(pageID));
			return getBuffer(hash.getValue(pageID));
		}
		int numFrame = freeList.get(0);
		System.out.println("--- Start The allocFrame of the BufferManager---");
		System.out.println("The numframe value is " + numFrame);
		hash.setKeyValue(pageID, numFrame);
		hash.getKey();
		hash.iter();
		pinPage(numFrame);
		freeList.remove((Integer) numFrame);
		_printList();
		// System.out.println("The bytebuffer of the frame is: "+
		// getBuffer(numFrame).getBufferFromFrame().hashCode());
		disk.readPage(pageID, getBuffer(numFrame));
		// System.out.println("The reapeted values is ");
		// System.out.println("The values for key  0 is " +hash.getValue(0));
		System.out.println("---End The allocFrame of the BufferManager---");
		System.out.println(" ");
		return getBuffer(numFrame);
	}

	private int getBufferNumber(int i) {
		return pool[i].getFrameNumber();
	}

	private Frame getBuffer(int i) {
		// System.out.println("---Start The getBuffer of the BufferManager---");
		// System.out.println("The Frame hascode is " + pool[i].hashCode());
		// System.out.println("---End The getBuffer of the BufferManager---");
		return pool[i];
	}

	/**
	 * pinPage only increases the relative pinCount variable of the Frame class
	 *
	 * @param frameNumber
	 */
	private void pinPage(int frameNumber) {
		pool[frameNumber].increasePincount();
	}

	/**
	 * unpinPage simply decrease the pinCount of the frame. It does not decrease
	 * the pinCount below 0.
	 *
	 * @param frameNumber
	 */
	private void unpinPage(int frameNumber) {
		// System.out.println("---Start unpinPage of BufferManager---");
		// System.out.println("The pincount before decreasing is "
		// + pool[frameNumber].getPinCount());
		if (pool[frameNumber].getPinCount() > 0)
			pool[frameNumber].decreasePincount();
		// System.out.println("The pincount before decreasing is "
		// + pool[frameNumber].getPinCount());
		// System.out
		// .println("Is the frame dirty? " + pool[frameNumber].isDirty());
		// System.out.println("---End unpinPage of BufferManager---");
	}

	/**
	 * Just changes the dirty and empty fields of the Frame class. This can be
	 * used to decide whether flush or not flush a page.
	 *
	 * @param frameNumber
	 */
	private void cleanPage(int frameNumber) {
		pool[frameNumber].setEmpty();
	}

	private boolean isBufferFull() {
		for (int i = 0; i <= pool.length; i++) {
			if (pool[i].getPinCount() == 0) return false;
		}
		return true;
	}

	private boolean isPage(int pageID) {
		return hash.hasKey(pageID);
	}

	// helpers
	private void _printList() {
		System.out.println("---Start Printing the list---s");
		for (Integer i : freeList)
			System.out.println(i);
		System.out.println("---End Printing the list---s");
	}

	public void _printHashMap() {
		hash.getKey();
		hash.iter();
	}

	public void _st(ByteBuffer b, DiskFile disk) throws IOException {
		disk.writePage(0, b);
	}

	// Just testing things
	public static void main1(String args[]) throws IOException {
		BufferManager a = BufferManager.getInstance();
		System.out.println("The freeList before allocation");
		a._printList();
		DiskFile disk;
		try {
			disk = new DiskFile("test.db");
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Can't access db file", e);
		}
		Frame sth = a.allocFrame(0, disk);
		Frame sec = a.allocFrame(1, disk);
		System.out.println("The sth hasCode is" + sth.hashCode());
		System.out.println("The freeList after allocation");
		a._printList();
		ByteBuffer b = sth.getBufferFromFrame();
		System.out.println("The b hasCode is" + b.hashCode());
		System.out.println("The buffer capacity is: " + b.capacity());
		System.out.println("The value is " + a.getBufferNumber(1));
		System.out.println("The buffers are " + a.isBufferFull());
		System.out.println("The pincount is " + sth.getPinCount());
		// for (int k = 0; k < 4; k++) {
		/* byte[] sth = a.getDataB(k); */
		/* ByteBuffer sth = a.getBuffer(k); */
		System.out.println("The hashcode is " + sth.hashCode());
		// System.out.println("The buffer with number: " + k);
		for (int i = 0; i < 16; i++) {
			System.out.println(" ");
			System.out.println(" Take " + i);
			System.out.println("The values is " + b.get(i));
			System.out.println("The limit is " + b.limit());
			System.out.println("The current position is " + b.position());
			System.out.println("The remaining value is " + b.remaining());
			System.out.println(" ");
		}
		/*
		 * for (int j = 0; j < 10; j++) {
		 * System.out.println("The elements are for: " + sth); }
		 */
		// }
		// it says zero but writes to 7(hardcoded)
		a.flushPage(0, disk);
		a.flushPage(1, disk);
		System.out.println("The pincount is " + sth.getPinCount());
		System.out.println("The empty buff is " + sth.isEmpty());
		System.out.println("The dirty buff is " + sth.isDirty());
		System.out.println("The freeList after reallocation");
		a._printList();
		System.out.println(" ");
		System.out.println("New stuff begins about ByteBuffers functionality.");
		// b.clear();// the loop prints the values
		for (int i = 0; i < 10; i++) {
			b.put(i, (byte) 4);
		}
		a._st(b, disk);
		System.out.println("The position is " + b.position());
		for (int i = 0; i < 16; i++) {
			System.out.println(" ");
			System.out.println("The values is " + b.get(i));
			System.out.println(" ");
		}
		/*
		 * b.flip(); //it allows for the buffer contents to be read again -
		 * without it the following loop prints nothing while (b.hasRemaining())
		 * System.out.println("The values is " + (byte)b.getInt());
		 */
	}
}
