package gr.uoa.di.mde515.files;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class IndexDiskFile extends DiskFile {

	public IndexDiskFile(String path) throws FileNotFoundException {
		super(path);
	}

	/**
	 * The method allows to read the contents of a disk block to a frame in
	 * memory. *
	 *
	 * @param pageID
	 * @param frame
	 * @throws IOException
	 */
	@Override
	public void readPage(int pageID, ByteBuffer buffer) throws IOException {
		super.readPage(-pageID - 1, buffer);
	}

	/**
	 * The method simply write the content of the buffer in the disk in the
	 * write offset.
	 *
	 * @param pageID
	 * @param buffer
	 * @throws IOException
	 */
	@Override
	public void writePage(int pageID, ByteBuffer buffer) throws IOException {
		super.writePage(-pageID - 1, buffer);
	}
}
