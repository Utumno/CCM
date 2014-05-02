package gr.uoa.di.mde515.engine.buffer;

import gr.uoa.di.mde515.index.Record;

import java.io.IOException;


public class HFDemo {
	// Creates the file header and inserts three records.
	// This test case involves the allocation of a new page,
	// insertion, until the page is full and then allocating a new one
	// to insert the third and final one. It also makes the necessary changes
	// to file and header pages.
	// The end result is saved in a file called test.db
	// In order to see the results you need a hex editor.
	// I use the HxD Hexeditor
	public static void main(String args[]) throws IOException {
		HF heapfile = new HF("temp.db");
		System.out.println("Creating the fileheader");
		heapfile.createFileHeader();
		System.out.println(" ");
		/*System.out.println("The pinning occurs here");
		Frame f = heapfile.buf().allocFrame(0);
		Page header = new Page(0, f.getBufferFromFrame());
		ByteBuffer b = header.getData();
		// Printing the file header
		System.out.println(" ");
		//System.out.println("The FREE LIST is " + b.getInt(0));
		//System.out.println("The FULL LIST is " + b.getInt(4));
		//System.out.println("The RECORD SIZE is " + b.getShort(8));
		System.out.println(" ");
		System.out.println("The numframe of the header is "
			+ f.getFrameNumber());
		System.out.println("Is it empty?  " + f.isEmpty());
		// create first PageHeader
		System.out.println("The lastpageid value is "
			+ DiskManager.last_allocated_pageID);
		System.out.println("Creating the first page");
		heapfile.createPageHeader(DiskManager.last_allocated_pageID + 1);
		heapfile.buf().printHashMap();
		System.out
			.println("The begining of allocframe after 1 createPageHeader");
		Frame first = heapfile.buf().allocFrame(1);
		Page firstpage = new Page(DiskManager.last_allocated_pageID + 1,
			first.getBufferFromFrame());
		ByteBuffer fir = firstpage.getData();
		System.out.println(" ");
		System.out.println("---HFDEMO---");
		System.out.println("The NEXT PAGE is " + fir.getInt(0));
		System.out.println("The PREVIOUS PAGE is " + fir.getInt(4));
		System.out.println("---HFDEMO---");
		System.out.println(" ");
		System.out
			.println("The lastpageid value is after pageheader creation is "
				+ DiskManager.last_allocated_pageID);
		// create second PageHeader
		System.out.println("The lastpageid value is "
			+ DiskManager.last_allocated_pageID);
		System.out.println("Creating the second page");
		heapfile.createPageHeader(DiskManager.last_allocated_pageID + 1);
		heapfile.buf().printHashMap();
		System.out
			.println("The begining of allocframe after 2 createPageHeader");
		Frame sec = heapfile.buf().allocFrame(2);
		Page secpage = new Page(DiskManager.last_allocated_pageID + 1,
			sec.getBufferFromFrame());
		ByteBuffer s = secpage.getData();
		System.out.println(" ");
		System.out.println("---HFDEMO---");
		System.out.println("The NEXT PAGE is " + s.getInt(0));
		System.out.println("The PREVIOUS PAGE is " + s.getInt(4));
		System.out.println("---HFDEMO---");
		System.out.println(" ");
		System.out
			.println("The lastpageid value is after pageheader creation is "
				+ DiskManager.last_allocated_pageID);
		heapfile.buf().printHashMap();*/
		System.out.println(" ");
		System.out.println(" ");
		System.out.println(" ");
		System.out.println("Writing the first record");
		Record<Integer, Integer> record = new Record<>(6,7);
		heapfile.insert(record);
		System.out.println(" ");
		System.out.println("Writing the second record");
		Record<Integer, Integer> record1 = new Record<>(8,9);
		heapfile.insert(record1);
		Record<Integer, Integer> record2 = new Record<>(10,11);
		heapfile.insert(record2);
		System.out.println("The insertion completed");
	}
}
