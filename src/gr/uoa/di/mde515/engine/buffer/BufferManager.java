package gr.uoa.di.mde515.engine.buffer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Kleomenis
 * 
 */
public class BufferManager {

	// the pool of frames
	private Frame[] pool;
	// the structure that maintain information about pageIDs and their
	// corresponding frame numbers
	private FramePage hash;
	// storage layer
	private DiskManager disk;
	// contains the available frames that can be used
	private List<Integer> freeList;

	private BufferManager(int numBufs) {
		// System.out.println("--- Start The BufferManager Constructor---");
		pool = new Frame[numBufs];
		hash = new FramePage();
		try {
			disk = new DiskManager("test.db");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		freeList = new ArrayList<Integer>();
		for (int i = 0; i < numBufs; i++) {
			pool[i] = new Frame(i);
			// System.out.println("The init hashcode is " + pool[i].hashCode());
			// System.out.println("The init ByteBuffer is "
			// + pool[i].getBufferFromFrame());
			freeList.add(i);
		}
		// System.out.println("---END The BufferManager Constructor---");
	}

	private static BufferManager instance = new BufferManager(4);

	// The BufferManager follows the singleton pattern. So one BufferManager is
	// created.
	public static BufferManager getInstance() {
		return instance;
	}

	public int getBufferNumber(int i) {
		return pool[i].getFrameNumber();
	}

	public Frame getBuffer(int i) {
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
	public void pinPage(int frameNumber) {
		pool[frameNumber].increasePincount();
	}

	/**
	 * unpinPage simply decrease the pinCount of the frame. It does not decrease
	 * the pinCount below 0.
	 * 
	 * @param frameNumber
	 */
	public void unpinPage(int frameNumber) {
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
	 * It is a wrapper around the DiskManager method
	 * 
	 * @param pageID
	 * @throws IOException
	 */
	public void newPage(int pageID) throws IOException {
		disk.allocateNewPage(pageID);
	}

	public void freePage(int frameNumber) {}

	/**
	 * It flushes the content of the frame assocciated with the given pageID to
	 * the disk at correct block.
	 * 
	 * @param pageID
	 * @throws IOException
	 */
	public void flushPage(int pageID) throws IOException {
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
		printList();
		// System.out.println("---End The flushPage of the BufferManager---");
	}

	/**
	 * Used by the file header to flush its contents to disk while the contents
	 * still remaining in the memory.
	 * 
	 * @throws IOException
	 */
	public void flushFileHeader() throws IOException {
		// if it does not exist?
		disk.writePage(0, pool[0].getBufferFromFrame());
		pool[0].setEmpty();
	}

	/**
	 * Just changes the dirty and empty fields of the Frame class. This can be
	 * used to decide whether flush or not flush a page.
	 * 
	 * @param frameNumber
	 */
	public void cleanPage(int frameNumber) {
		pool[frameNumber].setEmpty();
	}

	/**
	 * It first finds an empty buffer from the free list and then updates the
	 * map of pageIDs and frameNumbers along with returning the specified Frame.
	 * 
	 * @param pageID
	 * @return ByteBuffer
	 * @throws IOException
	 */
	public Frame allocFrame(int pageID) throws IOException {
		if (freeList.isEmpty()) {
			System.out.println("No available buffer");
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
		printList();
		// System.out.println("The bytebuffer of the frame is: "+
		// getBuffer(numFrame).getBufferFromFrame().hashCode());
		disk.readPage(pageID, getBuffer(numFrame));
		// System.out.println("The reapeted values is ");
		// System.out.println("The values for key  0 is " +hash.getValue(0));
		System.out.println("---End The allocFrame of the BufferManager---");
		System.out.println(" ");
		return getBuffer(numFrame);
	}

	public boolean isBufferFull() {
		for (int i = 0; i <= pool.length; i++) {
			if (pool[i].getPinCount() == 0) return false;
		}
		return true;
	}

	public boolean isPage(int pageID) {
		return hash.hasKey(pageID);
	}

	public void printList() {
		System.out.println("---Start Printing the list---s");
		for (Integer i : freeList)
			System.out.println(i);
		System.out.println("---End Printing the list---s");
	}

	public void printHashMap() {
		hash.getKey();
		hash.iter();
	}

	public void st(ByteBuffer b) throws IOException {
		disk.writePage(0, b);
	}

	// Just testing things
	public static void main(String args[]) throws IOException {
		BufferManager a = BufferManager.getInstance();
		System.out.println("The freeList before allocation");
		a.printList();
		Frame sth = a.allocFrame(0);
		Frame sec = a.allocFrame(1);
		System.out.println("The sth hasCode is" + sth.hashCode());
		System.out.println("The freeList after allocation");
		a.printList();
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
			System.out.println("The values is " + (byte) b.get(i));
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
		a.flushPage(0);
		a.flushPage(1);
		System.out.println("The pincount is " + sth.getPinCount());
		System.out.println("The empty buff is " + sth.isEmpty());
		System.out.println("The dirty buff is " + sth.isDirty());
		System.out.println("The freeList after reallocation");
		a.printList();
		System.out.println(" ");
		System.out.println("New stuff begins about ByteBuffers functionality.");
		// b.clear();// the loop prints the values
		for (int i = 0; i < 10; i++) {
			b.put(i, (byte) 4);
		}
		a.st(b);
		System.out.println("The position is " + b.position());
		for (int i = 0; i < 16; i++) {
			System.out.println(" ");
			System.out.println("The values is " + (byte) b.get(i));
			System.out.println(" ");
		}
		/*
		 * b.flip(); //it allows for the buffer contents to be read again -
		 * without it the following loop prints nothing while (b.hasRemaining())
		 * System.out.println("The values is " + (byte)b.getInt());
		 */
	}
}
