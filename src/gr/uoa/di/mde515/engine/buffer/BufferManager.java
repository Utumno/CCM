package gr.uoa.di.mde515.engine.buffer;

import gr.uoa.di.mde515.files.DiskFile;
import gr.uoa.di.mde515.index.PageId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class BufferManager<T> {

	private static final int NUM_BUFFERS = 35;
	// the pool of frames
	private final List<Frame> pool = new ArrayList<>(); // TODO unmodifiable
	// the structure that maintains information about pageIDs and their
	// corresponding frame numbers
	private final Map<T, Integer> pageIdToFrameNumber = new HashMap<>();
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

	public void setPageDirty(T pageID) {
		synchronized (POOL_LOCK) {
			getFrame(pageIdToFrameNumber.get(pageID)).setDirty(true);
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
			int frameNumber = pageIdToFrameNumber.get(pageID);
			System.out.println("The  PINCOUNT is "
				+ getFrame(frameNumber).getPinCount());
			decreasePinCount(frameNumber); // TODO public
			if (pool.get(frameNumber).isDirty())
				disk.writePage(pageID, pool.get(frameNumber)
					.getBufferFromFrame());
			getFrame(frameNumber).setDirty(false);
			pageIdToFrameNumber.remove(pageID); // TODO move to decreasePinCount
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
			// pool.get(0).setDirty(false);
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
			final Integer frameNum = pageIdToFrameNumber.get(pageID);
			if (frameNum != null) {
				increasePinCount(frameNum);
				return new Page<>(new PageId<>(pageID), getFrame(frameNum)
					.getBufferFromFrame());
			}
			while (freeList.isEmpty()) {
				System.out.println("No available buffer");
				POOL_LOCK.wait();
			}
			int numFrame = freeList.remove(0);
			pageIdToFrameNumber.put((T) pageID, numFrame);
			increasePinCount(numFrame);
			disk.readPage(pageID, getFrame(numFrame).getBufferFromFrame());
			return new Page<>(new PageId<>(pageID), getFrame(numFrame)
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
	public Page<Integer> allocFrameForNewPage(Integer pageID)
			throws InterruptedException {
		synchronized (POOL_LOCK) {
			while (freeList.isEmpty()) {
				System.out.println("No available buffer");
				POOL_LOCK.wait();
			}
			int numFrame = freeList.remove(0);
			pageIdToFrameNumber.put((T) pageID, numFrame);
			increasePinCount(numFrame); // FIXME same transaction should not
										// increase
			return new Page<>(new PageId<>(pageID), getFrame(numFrame)
				.getBufferFromFrame());
		}
	}

	/** Essentially the same as {@link #allocFrame(Integer, DiskFile)} */
	public Page<Integer> getAssociatedFrame(int pageID) {
		synchronized (POOL_LOCK) {
			return new Page<>(new PageId<>(pageID), getFrame(
				pageIdToFrameNumber.get(pageID)).getBufferFromFrame());
		}
	}

	private Frame getFrame(int i) {
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
				freeList.add(frameNumber);
				POOL_LOCK.notifyAll();
			}
		}
		return pinCount;
	}
}
