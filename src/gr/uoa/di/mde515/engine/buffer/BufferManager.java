package gr.uoa.di.mde515.engine.buffer;

import gr.uoa.di.mde515.files.DiskFile;
import gr.uoa.di.mde515.index.PageId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class BufferManager<T> {

	private static final int NUM_BUFFERS = 1000;
	/** the pool of frames - unmodifiable list */
	private final List<Frame> pool;
	/**
	 * the structure that maintains information about pageIDs and their
	 * corresponding frame numbers TODO bin - make pool to a map
	 */
	private final Map<T, Integer> pageIdToFrameNumber = new HashMap<>();
	/** contains the remaining available frames */
	private final List<Integer> freeList = new ArrayList<>();
	/** Contains the frames that need to remain pinned in memory */
	private final Set<T> pinPerm = new HashSet<>();
	private static final BufferManager<Integer> instance = new BufferManager<>(
		NUM_BUFFERS);
	/** All actions on the state fields must be performed using this lock */
	private static final Object POOL_LOCK = new Object();

	private BufferManager(int numBufs) {
		List<Frame> pool1 = new ArrayList<>();
		for (int i = 0; i < numBufs; i++) {
			pool1.add(new Frame(i));
			freeList.add(i);
		}
		// no additions ore deletions on the list of frames
		pool = Collections.unmodifiableList(pool1);
	}

	@SuppressWarnings("unused")
	// TODO
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
	 *            the id of the page to pin - MUST be an INT TODO
	 */
	public void pinPage(T pageID) {
		increasePinCount(pageIdToFrameNumber.get(pageID));
	}

	/**
	 * Decreases the pin count of the frame that corresponds to the
	 * {@code pageID} given. When the pin count reaches zero the page is added
	 * back to the free list.
	 *
	 * @param pageID
	 *            the id of the page to unpin - MUST be an INT TODO
	 */
	public void unpinPage(T pageID) {
		synchronized (POOL_LOCK) {
			final Integer frameNumber = pageIdToFrameNumber.get(pageID);
			System.out.println("Unpinned page - count: "
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

	/**
	 * Used when a transaction calls abort to unpin the page if dirty bypassing
	 * the {@link #pinPerm} in {@link #decreasePinCount(int, Object)}.
	 */
	public void killPage(T pageID) {
		synchronized (POOL_LOCK) {
			final Integer frameNum = pageIdToFrameNumber.get(pageID);
			final Frame frame = getFrame(frameNum);
			if (frame.isDirty()) {
				frame.resetPinCount();
				frame.setDirty(false);
				_free(pageID, frameNum);
			} else decreasePinCount(frameNum, pageID);
		}
	}

	/**
	 * MUST BE USED FROM SYNCHONIZED BLOCK. Move to {@link ReplacementAlgorithm}
	 *
	 * @param pageID
	 * @param frameNum
	 */
	private void _free(T pageID, final Integer frameNum) {
		pageIdToFrameNumber.remove(pageID);
		if (freeList.isEmpty()) {
			freeList.add(frameNum);
			POOL_LOCK.notifyAll();
		} else freeList.add(frameNum);
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
			final Frame frame = getFrame(frameNumber);
			if (frame.isDirty())
				disk.writePage(pageID, frame.getBufferFromFrame());
			frame.setDirty(false);
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

	public Page<T> allocPinPage(T pageID, DiskFile disk) throws IOException,
			InterruptedException {
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
			pageIdToFrameNumber.put(pageID, numFrame);
			pinPerm.add(pageID);
			pinPage(pageID);
			System.out.println("Is it empty? " + pinPerm.isEmpty());
			System.out.println("Does it contains the element? "
				+ pinPerm.contains(pageID));
			// FIXME cast below
			disk.readPage((int) pageID, getFrame(numFrame).getBufferFromFrame());
			return new Page<>(new PageId<>(pageID), getFrame(numFrame)
				.getBufferFromFrame());
		}
	}

	private Frame getFrame(int i) {
		synchronized (POOL_LOCK) {
			return pool.get(i);
		}
	}

	/**
	 * Increases the relative pinCount variable of the Frame class.
	 *
	 * @param frameNumber
	 */
	private void increasePinCount(int frameNumber) {
		getFrame(frameNumber).increasePincount(); // Frame.increasePincount is
		// an operations on an AtomicInteger - TODO do I need to synch on
		// POOL_LOCK ?
	}

	/**
	 * Decrease the pinCount of the frame. If the pin count of the frame reaches
	 * zero it adds it to the free list and notifiesAll(). TODO throw on
	 * negative pinCount
	 *
	 * @param frameNumber
	 * @return
	 */
	private int decreasePinCount(int frameNumber, T pageID) {
		int pinCount = -1;
		synchronized (POOL_LOCK) { // added this here to be double sure
			if ((!pinPerm.contains(pageID))
				&& (pinCount = pool.get(frameNumber).decreasePincount()) == 0)
				_free(pageID, frameNumber);
		}
		return pinCount;
	}
}
