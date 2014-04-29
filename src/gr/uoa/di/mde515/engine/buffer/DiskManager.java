package gr.uoa.di.mde515.engine.buffer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * @author Kleomenis
 * 
 */
public class DiskManager {

	// The page size constant
	private static int PAGE_SIZE = 32;
	private RandomAccessFile file;
	public static int last_allocated_pageID;
	// used to create a write a blank page in the disk
	// it is used by the allocateNewPage method
	public static final byte[] BLANK_PAGE = new byte[PAGE_SIZE];

	/**
	 * The constructor creates the database file
	 * 
	 * @param path
	 * @throws FileNotFoundException
	 */
	public DiskManager(String path) throws FileNotFoundException {
		file = new RandomAccessFile(path, "rw");
		// create as well the index file
		// last_allocated_pageID = 0;
	}

	// not used
	public void createDB(String path, int numPages) throws IOException {
		file = new RandomAccessFile(path, "rw");
	}

	/**
	 * The method allows to read the contents of a disk block to a frame in
	 * memory. *
	 * 
	 * @param pageID
	 * @param frame
	 * @throws IOException
	 */
	public void readPage(int pageID, Frame frame) throws IOException {
		// System.out.println("---Start readPage of DiskManager---");
		ByteBuffer data = frame.getBufferFromFrame();
		// System.out.println("---End readPage of DiskManager---");
		try {
			file.seek(pageID * PAGE_SIZE);
			file.read(data.array(), 0, PAGE_SIZE);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * The method simply write the content of the buffer in the disk in the
	 * write offset.
	 * 
	 * @param pageID
	 * @param buffer
	 * @throws IOException
	 */
	public void writePage(int pageID, ByteBuffer buffer) throws IOException {
		try {
			file.seek(pageID * PAGE_SIZE);
			file.write(buffer.array());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This method allows to increase the available pages of the file. It works
	 * by increasing additively to the already size file the number of pageID.
	 * 
	 * @param PageID
	 * @throws IOException
	 */
	public void allocateNewPage(int PageID) throws IOException {
		try {
			file.seek(PageID * PAGE_SIZE);
			file.write(BLANK_PAGE, 0, PAGE_SIZE);
		} catch (IOException e) {
			e.printStackTrace();
		}
		last_allocated_pageID = PageID;
	}

	public static void main(String args[]) throws IOException {
		DiskManager disk = new DiskManager("test.db");
		ByteBuffer b = ByteBuffer.allocate(PAGE_SIZE);
		Page p = new Page(0, b);
		for (int i = 0; i < PAGE_SIZE; i++) {
			p.writeByte(i, (byte) 1);
		}
		disk.writePage(0, b);
	}
}
