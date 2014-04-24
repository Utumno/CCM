package gr.uoa.di.mde515.engine;

interface Offsets {

	int RECORD_SIZE = 12;
	int PAGE_SIZE = 4096;
	long FILE_SIZE = PAGE_SIZE * 2000L;
	int numSlots = 100;
	int FILE_HEADER_LENGTH = 16;
	int FILE_FREE_LIST = 1;
	int FILE_FULL_LIST = 1;
	int NUM_RECORDS_HEADER_LOCATION = 0;
	int DATA_START_HEADER_LOCATION = 4;
	int PAGE_FREE_SPACE = 0;
	int PAGE_HEADER_NEXT = 2;
	int PAGE_HEADER_PREVIOUS = 4;
	int PAGE_HEADER_NUMSLOTS = 6;
	int PAGE_HEADER_SIZE = 1;
	int DATA_START = 6;
}
