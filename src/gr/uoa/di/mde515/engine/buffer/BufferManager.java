package gr.uoa.di.mde515.engine.buffer;

import gr.uoa.di.mde515.files.DiskFile;

import java.io.IOException;
import java.nio.ByteBuffer;
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
	private final Set<Integer> pinPerm = new HashSet<>();
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
	 * {@code pageID} given. Clients are responsible for unpinning their pages
	 * FIXME - what happens on abort commit transaction
	 *
	 * @param pageID
	 *            the id of the page to pin - MUST be an INT TODO
	 */
	public void pinPage(T pageID) {
		synchronized (POOL_LOCK) {
			getFrame(pageIdToFrameNumber.get(pageID)).increasePincount();
			// Frame.increasePincount is an operation on an AtomicInteger - TODO
			// do I really need to synch on POOL_LOCK ?
		}
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
			final Integer frameNumber = getFrameNum(pageID);
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
			final Integer frameNum = getFrameNum(pageID);
			final Frame frame = getFrame(frameNum);
			if (frame.isDirty()) {
				frame.resetPinCount();
				frame.setDirty(false);
				// pinPerm.remove(frameNum); // FIXME
				_free(pageID, frameNum);
			} else decreasePinCount(frameNum, pageID);
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
			final Frame frame = getFrame(frameNumber);
			if (frame.isDirty()) disk.writePage(pageID, frame.getBuffer());
			frame.setDirty(false);
		}
	}

	/**
	 * Returns a Page corresponding to an existing block of {@code file}, backed
	 * up by a frame in the main memory. It first checks if there is a Frame
	 * already allocated and if not it waits for an empty Frame from the free
	 * list and then updates the map of pageIDs and frameNumbers along with
	 * returning the specified Page. FIXME thread safe
	 *
	 * FIXME FIXME FIXME - let Lock manager know
	 *
	 * @param pageID
	 * @param disk
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public Page<T> allocFrame(T pageID, DiskFile disk) throws IOException,
			InterruptedException {
		synchronized (POOL_LOCK) {
			final Integer frameNum = getFrameNum(pageID);
			if (frameNum != null) { // if the page already in the buffer return
				// a page wrapping the buffer
				return new Page<>(pageID, getFrame(frameNum).getBuffer());
			}
			while (freeList.isEmpty()) {
				System.out.println("No available buffer");
				POOL_LOCK.wait();
			}
			int numFrame = freeList.remove(0);
			pageIdToFrameNumber.put(pageID, numFrame);
			final ByteBuffer buffer = getFrame(numFrame).getBuffer();
			disk.readPage((int) pageID, buffer);// FIXME cast
			return new Page<>(pageID, buffer);
		}
	}

	/**
	 * Returns a Page backed up by a frame in the main memory that does not
	 * correspond to block on the disk. May block waiting for a Frame to become
	 * available. FIXME FIXME FIXME - let Lock manager know
	 *
	 * @param pageID
	 * @throws InterruptedException
	 */
	public Page<T> allocFrameForNewPage(T pageID) throws InterruptedException {
		synchronized (POOL_LOCK) {
			while (freeList.isEmpty()) {
				System.out.println("No available buffer");
				POOL_LOCK.wait();
			}
			int numFrame = freeList.remove(0);
			pageIdToFrameNumber.put(pageID, numFrame);
			return new Page<>(pageID, getFrame(numFrame).getBuffer());
		}
	}

	public Page<T> allocPermanentPage(T pageID, DiskFile disk)
			throws IOException, InterruptedException {
		synchronized (POOL_LOCK) {
			final Integer frameNum = getFrameNum(pageID);
			if (frameNum != null) {
				final Frame frame = getFrame(frameNum);
				if (!pinPerm.contains(frameNum)) {
					// make it permanent
					frame.resetPinCount();
					frame.increasePincount(); // make pinCount 1
				}
				return new Page<>(pageID, frame.getBuffer());
			}
			while (freeList.isEmpty()) {
				System.out.println("No available buffer");
				POOL_LOCK.wait();
			}
			int numFrame = freeList.remove(0);
			pageIdToFrameNumber.put(pageID, numFrame);
			pinPerm.add(frameNum);
			System.out.println("pinPerm " + pinPerm);
			// pinPage(pageID);
			final ByteBuffer buffer = getFrame(numFrame).getBuffer();
			disk.readPage((int) pageID, buffer);// FIXME cast
			return new Page<>(pageID, buffer);
		}
	}

	// =========================================================================
	// Private helpers - all nust be called holding the POOL_LOCK TODO
	// =========================================================================
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
	 * @param pageID
	 * @return the Frame for this pid or null if none
	 */
	private Integer getFrameNum(T pageID) {
		final Integer frameNum = pageIdToFrameNumber.get(pageID);
		return frameNum;
	}

	private Frame getFrame(int i) {
		synchronized (POOL_LOCK) {
			return pool.get(i);
		}
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
			if ((!pinPerm.contains(getFrame(frameNumber)))
				&& (pinCount = getFrame(frameNumber).decreasePincount()) == 0)
				_free(pageID, frameNumber);
		}
		return getFrame(frameNumber).getPinCount().intValue();
	}
}
