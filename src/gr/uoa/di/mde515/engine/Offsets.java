package gr.uoa.di.mde515.engine;

//
interface Offsets {

	final int RECORD_SIZE = 12;
	final int PAGE_SIZE = 4096;
	final long FILE_SIZE = PAGE_SIZE * 2000L;
	final int numSlots = 100;	
	final int FILE_HEADER_LENGTH = 16;
	final int FILE_FREE_LIST = 1;
	final int FILE_FULL_LIST = 1;
	final int NUM_RECORDS_HEADER_LOCATION = 0;
	final int DATA_START_HEADER_LOCATION = 4;
	final int PAGE_FREE_SPACE = 0;
	final int PAGE_HEADER_NEXT = 2;
	final int PAGE_HEADER_PREVIOUS = 4;
	final int PAGE_HEADER_NUMSLOTS = 6;
	final int PAGE_HEADER_SIZE = 1;
	final int DATA_START = 6;
}
