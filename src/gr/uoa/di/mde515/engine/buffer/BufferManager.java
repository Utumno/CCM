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

public final class BufferManager {

	private static final int NUM_BUFFERS = 200;
	/** the pool of frames - unmodifiable list */
	private final List<Frame> pool;
	/**
	 * the structure that maintains information about pageIDs and their
	 * corresponding frame numbers TODO bin - make pool to a map
	 */
	private final Map<Integer, Integer> pageIdToFrameNumber = new HashMap<>();
	/** contains the remaining available frames */
	private final List<Integer> freeList = new ArrayList<>();
	/** Contains the frames that need to remain pinned in memory */
	private final Set<Integer> pinPerm = new HashSet<>();
	private static final BufferManager instance = new BufferManager(NUM_BUFFERS);
	/**
	 * All actions on the state fields must be performed holding this lock. Also
	 * all writes to byte buffers must be performed holding this lock (TODO
	 * enough for readers ?)
	 */
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
	private static enum ReplacementAlgorithm {
		LRU; // TODO
	}

	/**
	 * The BufferManager follows the singleton pattern. So one BufferManager is
	 * created. TODO proper generic factory !
	 */
	public static BufferManager getInstance() {
		return instance;
	}

	// =========================================================================
	// API
	// =========================================================================
	public void setPageDirty(int pageID) {
		synchronized (POOL_LOCK) {
			getFrame(pageIdToFrameNumber.get(pageID)).setDirty(true);
		}
	}

	/**
	 * Increases the pin count of the frame that corresponds to the
	 * {@code pageID} given. Clients are responsible for unpinning their pages
	 * FIXME - what happens on abort commit transaction
	 *
	 * @param pageID
	 *            the id of the page to pin - MUST be an INT TODO
	 */
	public void pinPage(int pageID) {
		synchronized (POOL_LOCK) {
			final Integer i = pageIdToFrameNumber.get(pageID);
			final Frame frame = getFrame(i);
			frame.increasePincount();
			// System.out.println("pinPage::Frame num " + i + " pin count "
			// + frame.getPinCount());
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
	public void unpinPage(int pageID) {
		synchronized (POOL_LOCK) {
			final Integer frameNumber = getFrameNum(pageID);
			decreasePinCount(frameNumber, pageID);
		}
	}

	/**
	 * Used when a transaction calls abort to unpin the page if dirty bypassing
	 * the {@link #pinPerm} in {@link #decreasePinCount(int, Object)}.
	 *
	 * @throws IOException
	 *             thrown in the case a permanent generation page must be
	 *             cleaned which can only be done by rereading it from disk
	 */
	public void killPage(int pageID, DiskFile file) throws IOException {
		synchronized (POOL_LOCK) {
			final Integer frameNum = getFrameNum(pageID);
			final Frame frame = getFrame(frameNum);
			if (frame.isDirty()) {
				if (pinPerm.contains(pageID)) {
					file.readPage(pageID, frame.getBuffer());
				} else {
					frame.resetPinCount();
					frame.setDirty(false);
					// pinPerm.remove(frameNum); // FIXME
					_free(pageID, frameNum);
				}
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
	 * already allocated and if not it waits for an empty Frame
	 *
	 * FIXME FIXME FIXME - let Lock manager know
	 *
	 * @param pageID
	 * @param file
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public Page allocFrame(int pageID, DiskFile file) throws IOException,
			InterruptedException {
		synchronized (POOL_LOCK) {
			final Integer frameNum = getFrameNum(pageID);
			if (frameNum != null) { // if the page already in the buffer return
				// a page wrapping the buffer
				// System.out.println("re-alloc FRAME NUM " + frameNum
				// + " for page " + pageID);
				return new Page(pageID, getFrame(frameNum).getBuffer());
			}
			while (freeList.isEmpty()) {
				System.out.println("No available buffer");
				POOL_LOCK.wait();
			}
			int numFrame = freeList.remove(0);
			// System.out.println("alloc FRAME NUM " + numFrame + " for page "
			// + pageID);
			pageIdToFrameNumber.put(pageID, numFrame);
			final ByteBuffer buffer = getFrame(numFrame).getBuffer();
			file.readPage(pageID, buffer);
			return new Page(pageID, buffer);
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
	public Page allocFrameForNewPage(int pageID) throws InterruptedException {
		synchronized (POOL_LOCK) {
			while (freeList.isEmpty()) {
				System.out.println("No available buffer");
				POOL_LOCK.wait();
			}
			int numFrame = freeList.remove(0);
			// System.out.println("ALLOC new page FRAME NUM " + numFrame
			// + " for page " + pageID);
			pageIdToFrameNumber.put(pageID, numFrame);
			return new Page(pageID, getFrame(numFrame).getBuffer());
		}
	}

	/** Better testing TODO */
	public Page allocPermanentPage(int pageID, DiskFile disk)
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
				return new Page(pageID, frame.getBuffer());
			}
			while (freeList.isEmpty()) {
				System.out.println("No available buffer");
				POOL_LOCK.wait();
			}
			Integer numFrame = freeList.remove(0);
			System.out
				.println("alloc perm " + numFrame + " for page " + pageID);
			pageIdToFrameNumber.put(pageID, numFrame);
			pinPerm.add(numFrame);
			System.out.println("pinPerm " + pinPerm);
			// pinPage(pageID);
			final ByteBuffer buffer = getFrame(numFrame).getBuffer();
			disk.readPage(pageID, buffer);
			return new Page(pageID, buffer);
		}
	}

	// =========================================================================
	// Private helpers - all must be called holding the POOL_LOCK
	// =========================================================================
	/**
	 * MUST BE USED FROM SYNCHONIZED BLOCK. Move to {@link ReplacementAlgorithm}
	 *
	 * @param pageID
	 * @param frameNum
	 */
	private void _free(int pageID, final Integer frameNum) {
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
	private Integer getFrameNum(int pageID) {
		final Integer frameNum = pageIdToFrameNumber.get(pageID);
		return frameNum;
	}

	private Frame getFrame(int i) {
		return pool.get(i);
	}

	/**
	 * Decrease the pinCount of the frame. If the pin count of the frame reaches
	 * zero it adds it to the free list and notifiesAll(). TODO throw on
	 * negative pinCount
	 *
	 * @param frameNumber
	 * @return the current pin count of the page
	 */
	private int decreasePinCount(int frameNumber, int pageID) {
		final Frame frame = getFrame(frameNumber);
		if ((!pinPerm.contains(frame)) // TODO move to replacement algorithm
			&& (frame.decreasePincount()) == 0) _free(pageID, frameNumber);
		return frame.getPinCount().intValue();
	}
}
