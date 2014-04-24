package gr.uoa.di.mde515.engine.buffer;

import java.nio.ByteBuffer;

public class Frame {

	private static final int PAGE_SIZE = 4096;
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

	public byte[] getBuffer() {
		return data.array();
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
	}

	public void setEmpty() {
		empty = true;
	}
}
