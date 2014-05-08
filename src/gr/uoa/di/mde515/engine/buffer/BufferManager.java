package gr.uoa.di.mde515.engine.buffer;

import gr.uoa.di.mde515.files.DiskFile;
import gr.uoa.di.mde515.index.PageId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class BufferManager<T> {

	private static final int NUM_BUFFERS = 10;
	// the pool of frames
	private final List<Frame> pool = new ArrayList<>(); // TODO unmodifiable
	// the structure that maintains information about pageIDs and their
	// corresponding frame numbers
	private final Map<T, Integer> map = new HashMap<>();
	// contains the available frames that can be used
	private final List<Integer> freeList = new ArrayList<>();
	private static final BufferManager<Integer> instance = new BufferManager<>(
		NUM_BUFFERS);
	private static final Object POOL_LOCK = new Object();

	private BufferManager(int numBufs) {
		for (int i = 0; i < numBufs; i++) {
			pool.add(new Frame(i));
			freeList.add(i);
		}
	}

	@SuppressWarnings("unused")
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
	public static BufferManager<Integer> getInstance() {
		return instance;
	}

	public void setFrameDirty(T pageID) {
		synchronized (POOL_LOCK) {
			int frameNumber = map.get(pageID);
			getBuffer(frameNumber).setDirty();
		}
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
		synchronized (POOL_LOCK) {
			int frameNumber = map.get(pageID);
			decreasePinCount(frameNumber);
			if (pool.get(frameNumber).isDirty())
				disk.writePage(pageID, pool.get(frameNumber)
					.getBufferFromFrame());
			cleanPage(frameNumber);
			map.remove(pageID); // TODO move to decreasePinCount
		}
	}

	/**
	 * Flushes the header of the file while keeping it pinned in main memory.
	 *
	 * @param disk
	 * @throws IOException
	 */
	public void flushFileHeader(DiskFile disk) throws IOException {
		// if it does not exist?
		synchronized (POOL_LOCK) {
			disk.writePage(0, pool.get(0).getBufferFromFrame());
			pool.get(0).setEmpty();
		}
	}

	/**
	 * Returns a Page corresponding to an existing block of {@code file}, backed
	 * up by a frame in the main memory. It first finds an empty buffer from the
	 * free list and then updates the map of pageIDs and frameNumbers along with
	 * returning the specified Page. FIXME thread safe
	 *
	 * FIXME FIXME FIXME - let Lock manager know
	 *
	 * @param pageID
	 * @param disk
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public Page<Integer> allocFrame(Integer pageID, DiskFile disk)
			throws IOException, InterruptedException {
		synchronized (POOL_LOCK) {
			/* if the page already in the buffer return the buffer */
			final Integer frameNum = map.get(pageID);
			if (frameNum != null) {
				System.out.println("FRAME NUM " + frameNum);
				increasePinCount(frameNum); // FIXME transaction
				final ByteBuffer bufferFromFrame = getBuffer(frameNum)
					.getBufferFromFrame();
				System.out.println("Buffer "
					+ Arrays.toString(bufferFromFrame.array()));
				return new Page<>(new PageId<>(pageID), bufferFromFrame);
			}
			while (freeList.isEmpty()) {
				System.out.println("No available buffer");
				POOL_LOCK.wait();
			}
			int numFrame = freeList.remove(0);
			map.put((T) pageID, numFrame);
			increasePinCount(numFrame);
			disk.readPage(pageID, getBuffer(numFrame).getBufferFromFrame());
			return new Page<>(new PageId<>(pageID), getBuffer(numFrame)
				.getBufferFromFrame());
		}
	}

	/**
	 * Returns a Page backed up by a frame in the main memory. It first finds an
	 * empty buffer from the free list and then updates the map of pageIDs and
	 * frameNumbers along with returning the specified Page. FIXME thread safe
	 *
	 * FIXME FIXME FIXME - let Lock manager know
	 *
	 * @param pageID
	 * @throws InterruptedException
	 */
	public Page<Integer> allocFrameForNewPage(Integer pageID) throws InterruptedException {
		synchronized (POOL_LOCK) {
			while (freeList.isEmpty()) {
				System.out.println("No available buffer");
				POOL_LOCK.wait();
			}
			int numFrame = freeList.remove(0);
			map.put((T) pageID, numFrame);
			increasePinCount(numFrame); // FIXME same tarnsaction
			return new Page<>(new PageId<>(pageID), getBuffer(numFrame)
				.getBufferFromFrame());
		}
	}

	private Frame getBuffer(int i) {
		return pool.get(i);
	}

	/**
	 * Increases the relative pinCount variable of the Frame class.
	 *
	 * @param frameNumber
	 */
	private void increasePinCount(int frameNumber) {
		pool.get(frameNumber).increasePincount();
	}

	/**
	 * Decrease the pinCount of the frame. MuST BE USED FROM SYNCHONIZED BLOCK.
	 * If the pin count of the frame reaches zero it adds it to the free list
	 * and notifiesAll(). TODO throw on negative pinCount
	 *
	 * @param frameNumber
	 * @return
	 */
	private int decreasePinCount(int frameNumber) {
		final Frame frame = pool.get(frameNumber);
		final int pinCount = frame.decreasePincount();
		if (pinCount == 0) {
			if (freeList.isEmpty()) {
				POOL_LOCK.notifyAll();
			}
			freeList.add(frameNumber);
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
}
