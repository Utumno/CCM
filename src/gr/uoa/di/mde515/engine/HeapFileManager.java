import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.io.RandomAccessFile;

public class HeapFileManager {
	HeapFile file;
	Page fileheader;
	
	public HeapFileManager(){
		file = new HeapFile("customers.txt");
		fileheader = file.readPage(0);
	}
	
	public void checkIfNew(Page fileheader){
		
	}
}
