package gr.uoa.di.mde515.engine.buffer;

import java.nio.ByteBuffer;

public class Frame {

	private static final int PAGE_SIZE = 32;
	private int framenumber;
	private int pincount;
	private boolean dirty;
	private boolean empty;
	private ByteBuffer data;

	public Frame(int i) {
		framenumber = i;
		pincount = 0;
		dirty = false;
		empty = true;
		data = ByteBuffer.allocate(PAGE_SIZE);
	}

	public int getPinCount() {
		return pincount;
	}

	public int getFrameNumber() {
		return framenumber;
	}

	public ByteBuffer getBufferFromFrame() {
		// System.out.println("Start getBufferFromFrame");
		// System.out.println("The (getBufferFromFrame)ByteBuffer is "
		// + data.hashCode());
		// System.out.println("End getBufferFromFrame");
		return data;
	}

	public boolean isDirty() {
		return dirty;
	}

	public boolean isEmpty() {
		return empty;
	}

	public void increasePincount() {
		pincount++;
	}

	public void decreasePincount() {
		pincount--;
	}

	public void setDirty() {
		dirty = true;
		empty = false;
	}

	public void setEmpty() {
		empty = true;
		dirty = false;
	}
}
