package gr.uoa.di.mde515.files;

import gr.uoa.di.mde515.engine.buffer.Frame;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * @author Kleomenis
 */
public class DiskFile {

	// TODO CLOSE THE FILE
	// The page size constant
	private static int PAGE_SIZE = 48; // TODO globals
	private RandomAccessFile file;
	public static int last_allocated_pageID;
	// used to create a write a blank page in the disk
	// it is used by the allocateNewPage method
	public static final byte[] BLANK_PAGE = new byte[PAGE_SIZE];

	/**
	 * The constructor creates the database file or opens it if already exists
	 *
	 * @param path
	 * @throws FileNotFoundException
	 */
	public DiskFile(String path) throws FileNotFoundException {
		file = new RandomAccessFile(path, "rw");
		// create as well the index file
		// last_allocated_pageID = 0;
	}

	/**
	 * The method allows to read the contents of a disk block to a frame in
	 * memory. *
	 *
	 * @param pageID
	 * @param frame
	 * @throws IOException
	 */
	public void readPage(int pageID, ByteBuffer buffer) throws IOException {
		// System.out.println("---Start readPage of DiskManager---");
		file.seek(pageID * PAGE_SIZE);
		file.read(buffer.array(), 0, PAGE_SIZE);
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
		file.seek(pageID * PAGE_SIZE);
		file.write(buffer.array());
	}

	/**
	 * This method allows to increase the available pages of the file. It works
	 * by increasing additively to the already size file the number of pageID.
	 *
	 * @param PageID
	 * @throws IOException
	 */
	public void allocateNewPage(int PageID) throws IOException {
		file.seek(PageID * PAGE_SIZE);
		file.write(BLANK_PAGE, 0, PAGE_SIZE);
		last_allocated_pageID = PageID;
	}

	public void close() throws IOException {
		file.close();
	}
}
