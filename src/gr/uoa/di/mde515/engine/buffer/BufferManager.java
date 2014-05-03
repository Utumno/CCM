package gr.uoa.di.mde515.engine.buffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Kleomenis
 */
public final class BufferManager {

	private static final int NUM_BUFFERS = 10;
	// the pool of frames
	private final List<Frame> pool = new ArrayList<>(); // TODO unmodifiable
	// the structure that maintains information about pageIDs and their
	// corresponding frame numbers
	private final FramePage hash = new FramePage();
	// contains the available frames that can be used
	private final List<Integer> freeList = new ArrayList<>();
	private static final BufferManager instance = new BufferManager(NUM_BUFFERS);
	private static final Object POOL_LOCK = new Object();

	private BufferManager(int numBufs) {
		for (int i = 0; i < numBufs; i++) {
			pool.add(new Frame(i));
			freeList.add(i);
		}
	}

	private static enum ReplacementAlgorithm {
		LRU;
	}

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
		decreasePinCount(frameNumber);
		if (pool.get(frameNumber).isDirty())
			disk.writePage(pageID, pool.get(frameNumber).getBufferFromFrame());
		cleanPage(frameNumber);
		hash.removeKey(pageID);
	}

	/**
	 * Flushes the header of the file while keeping it pinned in main memory.
	 *
	 * @param disk
	 * @throws IOException
	 */
	public void flushFileHeader(DiskFile disk) throws IOException {
		// if it does not exist?
		disk.writePage(0, pool.get(0).getBufferFromFrame());
		pool.get(0).setEmpty();
	}

	/**
	 * Returns a Frame It first finds an empty buffer from the free list (TODO:
	 * list full) and then updates the map of pageIDs and frameNumbers along
	 * with returning the specified Frame. FIXME thread safe
	 *
	 * FIXME FIXME FIXME - let Lock manager know
	 *
	 * @param pageID
	 * @param disk
	 * @return ByteBuffer
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public Frame allocFrame(int pageID, DiskFile disk) throws IOException,
			InterruptedException {
		synchronized (POOL_LOCK) {
			while (freeList.isEmpty()) {
				System.out.println("No available buffer");
				POOL_LOCK.wait();
			}
			/* if the page already in the buffer return the buffer */
			if (isPage(pageID)) {
				final int frameNum = hash.getValue(pageID);
				pinPage(frameNum);
				return getBuffer(frameNum);
			}
			int numFrame = freeList.get(0);
			hash.setKeyValue(pageID, numFrame);
			hash.print();
			pinPage(numFrame);
			freeList.remove((Integer) numFrame);
			disk.readPage(pageID, getBuffer(numFrame));
			return getBuffer(numFrame);
		}
	}

	private Frame getBuffer(int i) {
		return pool.get(i);
	}

	/**
	 * pinPage only increases the relative pinCount variable of the Frame class
	 *
	 * @param frameNumber
	 */
	private void pinPage(int frameNumber) {
		pool.get(frameNumber).increasePincount();
	}

	/**
	 * Decrease the pinCount of the frame. If the pin count of the frame reaches
	 * zero it adds it to the free list and notifiesAll(). TODO throw on
	 * negative pinCount
	 *
	 * @param frameNumber
	 * @return
	 */
	private int decreasePinCount(int frameNumber) {
		final Frame frame = pool.get(frameNumber);
		final int pinCount = frame.decreasePincount();
		if (pinCount == 0) {
			synchronized (POOL_LOCK) {
				if (freeList.isEmpty()) {
					POOL_LOCK.notifyAll();
				}
				freeList.add(frameNumber);
			}
		}
		return pinCount;
	}

	/**
	 * Just changes the dirty and empty fields of the Frame class. This can be
	 * used to decide whether flush or not flush a page.
	 *
	 * @param frameNumber
	 */
	private void cleanPage(int frameNumber) {
		pool.get(frameNumber).setEmpty();
	}

	private boolean isPage(int pageID) {
		return hash.hasKey(pageID);
	}
}
