package gr.uoa.di.mde515.engine.buffer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/** A wrapper around a ByteBuffer representing a frame in the main memory */
final class Frame {

	private static final int PAGE_SIZE = 48;
	private final int framenumber;
	private final AtomicInteger pincount = new AtomicInteger();
	private volatile boolean dirty;
	private final ByteBuffer data;

	public Frame(int i) {
		framenumber = i;
		dirty = false;
		data = ByteBuffer.allocate(PAGE_SIZE);
	}

	public int getFrameNumber() {
		return framenumber;
	}

	public ByteBuffer getBufferFromFrame() {
		// System.out.println("Start getBufferFromFrame");
		// System.out.println("The framenumber is " + getFrameNumber());
		// System.out.println("The (getBufferFromFrame)ByteBuffer is "
		// + data.hashCode());
		// System.out.println("End getBufferFromFrame");
		return data;
	}

	public int printByteBufferHashCode() {
		return data.hashCode();
	}

	public boolean isDirty() {
		return dirty;
	}

	public int increasePincount() {
		return pincount.incrementAndGet();
	}

	public int decreasePincount() {
		return pincount.decrementAndGet();
	}

	public void setDirty(boolean dirty) {
		this.dirty = dirty;
	}

	public AtomicInteger getPinCount() {
		return pincount;
	}

	public void resetPinCount() {
		pincount.set(0);
	}
}
