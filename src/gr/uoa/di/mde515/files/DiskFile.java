package gr.uoa.di.mde515.files;

import gr.uoa.di.mde515.engine.Engine;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class DiskFile {

	private static int PAGE_SIZE = Engine.PAGE_SIZE;
	private final RandomAccessFile file;

	/**
	 * The constructor creates the database file or opens it if already exists
	 *
	 * @param path
	 * @throws FileNotFoundException
	 */
	public DiskFile(String path) throws FileNotFoundException {
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
	public void readPage(int pageID, ByteBuffer buffer) throws IOException {
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

	public void close() throws IOException {
		file.close();
	}

	/** Used to check if file exists */
	public int read() throws IOException {
		return file.read();
	}
}
