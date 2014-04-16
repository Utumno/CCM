package gr.uoa.di.mde515.engine;

import java.io.*;
import java.nio.*;

public class MyFile {
		
	private static RandomAccessFile file;
	//endeiktikes times offsets 
	static int FILE_HEADER_LENGTH = 16;	
	static int NUM_OF_RECORDS = 16;	
	static int NUM_RECORDS_HEADER_LOCATION = 0;
	static int DATA_START_HEADER_LOCATION = 4;
	static int sizeofRecord = 1;
	static int FIRST_FREE_RECORD = 8;
	static int BITMAP = 8;
	
	public MyFile() throws IOException {
		
		file = new RandomAccessFile("file.txt", "rw");
		file.setLength(1000);
		
	}
		
	public Page getPage(int pageid) throws IOException {
		
		byte [] d = new byte[4096]; // normally, we use a buffer from the pool
		file.seek(pageid);
		file.read(d);
		Page apage = new Page(pageid, d);
		return apage;		
	}	
	
	//Instead of RandomAccessFile methods, write to buffer and flush the buffer to the file
	public void createFileHeader() throws IOException {
			
		//file.seek(NUM_RECORDS_HEADER_LOCATION);
		//file.writeInt(NUM_OF_RECORDS);
		//file.seek(DATA_START_HEADER_LOCATION);
		//file.writeInt( FILE_HEADER_LENGTH +1);
		Page filehdr = getPage(0);
		filehdr.writeInt(NUM_RECORDS_HEADER_LOCATION, 5);
		filehdr.writeInt(DATA_START_HEADER_LOCATION, 10);
		synchPageToFile(filehdr);
	}
	
	public void insertRecord() {
		//TODO
	}
	
	// again the ByteBuffer could be used instead of seek
	public void deleteRecord(int pageid, int slot) {
		//TODO
		//file.seek(pageid+slot);
		//file.write();
	}
	
	public void searchForRecord(int pageid, int slot) throws IOException {
			file.seek(pageid+(slot*sizeofRecord));
			System.out.println("The record is "+ file.readInt()); // according to the record structure
	}
	
	public static void synchPageToFile(Page apage) throws IOException{
		ByteBuffer data = apage.getData(); 
		int pageid = apage.getPageId();
		if (data!= null) {
			file.seek(pageid);
			file.write(data.array());
		}
	}
	
	public static void main(String args[]) {
		
		try {
			MyFile obj = new MyFile();
			obj.createFileHeader();
			// checks if the contents are written to the file
			file.seek(0);
			System.out.println("The header value from file is "+file.readInt());
			file.seek(4);
			System.out.println("The data value from file is "+file.readInt());
		}
		catch (IOException e){
			 System.err.println("Caught IOException df: " + e.getMessage());
		}
	}
}