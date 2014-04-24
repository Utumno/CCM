package gr.uoa.di.mde515.engine.buffer;

import java.awt.Frame;
import java.io.RandomAccessFile;

public class BufferManager {

	private Frame[] pool;
	private FramePage hash;
	private RandomAccessFile dbfile;

	private BufferManager(int numBufs) {
		pool = new Frame[numBufs];
		for (int i = 0; i < numBufs; i++) {
			pool[i] = new Frame(i);
		}
		hash = new FramePage();
	}

	private static BufferManager instance = new BufferManager(4);

	public static BufferManager getInstance() {
		return instance;
	}

	public int getBufferNumber(int i) {
		return pool[i].getFrameNumber();
	}

	public byte[] getDataB(int i) {
		System.out.println("The hascoded is " + pool[i].getBuffer().hashCode());
		return pool[i].getBuffer();
	}

	public void pinPage(Page pagenum) {}

	public void unpinPage(Page pagenum) {}

	public void newPage(Page firstpage) {}

	public void freePage(Page pagenum) {}

	public void flushPage(Page pagenum) {}

	public void allocBuf() {
		int numframe;
		for (numframe = 0; numframe < 4; numframe++) {
			if (pool[numframe].isEmpty()) break;
		}
		System.out.println("The numframe value is " + numframe);
		hash.setKeyValue(1, 0);
		hash.setKeyValue(2, 1);
		hash.setKeyValue(3, 2);
		hash.setKeyValue(4, 3);
	}

	public void printHashMap() {
		System.out.println("The hashmap value should be " + hash.toString());
		System.out.println("The (2) hashmap contains key " + hash.hasKey(1));
		System.out.println("The (3) hashmap value should be "
			+ hash.hasValue(1));
		System.out.println();
		hash.iter();
	}
}
