package gr.uoa.di.mde515.engine.buffer;

import gr.uoa.di.mde515.files.DiskFile;
import gr.uoa.di.mde515.index.PageId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class BufferManager<T> {

	private static final int NUM_BUFFERS = 35;
	/** the pool of frames */
	private final List<Frame> pool = new ArrayList<>(); // TODO unmodifiable
	// the structure that maintains information about pageIDs and their
	// corresponding frame numbers
	private final Map<T, Integer> pageIdToFrameNumber = new HashMap<>();
	// contains the available frames that can be used
	private final List<Integer> freeList = new ArrayList<>();
	// contains the frames that need to remain pinned in memory
	private final Set<Integer> pinPerm = new HashSet<>();
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
	 * Increases the pin count of the frame that corresponds to the
	 * {@code pageID} given.
	 *
	 * @param pageID
	 *            the id of the page to pin - an integer probably
	 */
	public void pinPage(T pageID) {
		increasePinCount(pageIdToFrameNumber.get(pageID));
	}

	/**
	 * Decreases the pin count of the frame that corresponds to the
	 * {@code pageID} given.
	 *
	 * @param pageID
	 *            the id of the page to unpin - an integer probably
	 */
	public void unpinPage(T pageID) {
		synchronized (POOL_LOCK) {
			final Integer frameNumber = pageIdToFrameNumber.get(pageID);
			System.out.println("FRAME NUM " + frameNumber);
			System.out.println("The PIN is"
				+ decreasePinCount(frameNumber, pageID));
		}
	}

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

	public void setPageEmtpy(T pageID) {
		synchronized (POOL_LOCK) {
			getFrame(pageIdToFrameNumber.get(pageID)).setDirty(false);
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
			if (pool.get(frameNumber).isDirty())
				disk.writePage(pageID, pool.get(frameNumber)
					.getBufferFromFrame());
			getFrame(frameNumber).setDirty(false);
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
			// if the page already in the buffer return the buffer
			final Integer frameNum = pageIdToFrameNumber.get(pageID);
			if (frameNum != null) {
				return new Page<>(new PageId<>(pageID), getFrame(frameNum)
					.getBufferFromFrame());
			}
			while (freeList.isEmpty()) {
				System.out.println("No available buffer");
				POOL_LOCK.wait();
			}
			int numFrame = freeList.remove(0);
			pageIdToFrameNumber.put((T) pageID, numFrame);
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
			return new Page<>(new PageId<>(pageID), getFrame(numFrame)
				.getBufferFromFrame());
		}
	}

	public Page<Integer> allocPinPage(Integer pageID, DiskFile disk)
			throws IOException, InterruptedException {
		synchronized (POOL_LOCK) {
			/* if the page already in the buffer return the buffer */
			final Integer frameNum = pageIdToFrameNumber.get(pageID);
			if (frameNum != null) {
				return new Page<>(new PageId<>(pageID), getFrame(frameNum)
					.getBufferFromFrame());
			}
			while (freeList.isEmpty()) {
				System.out.println("No available buffer");
				POOL_LOCK.wait();
			}
			int numFrame = freeList.remove(0);
			pageIdToFrameNumber.put((T) pageID, numFrame);
			pinPerm.add(pageID);
			System.out.println("Is it empty? " + pinPerm.isEmpty());
			System.out.println("Does it contains the element? "
				+ pinPerm.contains(pageID));
			disk.readPage(pageID, getFrame(numFrame).getBufferFromFrame());
			return new Page<>(new PageId<>(pageID), getFrame(numFrame)
				.getBufferFromFrame());
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
	private int decreasePinCount(int frameNumber, T pageID) {
		final Frame frame = pool.get(frameNumber);
		final int pinCount = frame.decreasePincount();
		if ((pinCount == 0) && (!pinPerm.contains(pageID))) {
			pageIdToFrameNumber.remove(pageID);
			freeList.add(frameNumber);
			if (freeList.isEmpty()) {
				POOL_LOCK.notifyAll();
			}
		}
		return pinCount;
	}
}
