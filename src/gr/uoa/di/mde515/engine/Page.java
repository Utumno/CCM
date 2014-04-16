package gr.uoa.di.mde515.engine;

import java.nio.ByteBuffer;
import java.io.RandomAccessFile;

public class Page {
		
		public int pageid;
		private ByteBuffer data;
		public boolean dirty = false;
		//auta prepei na mpoun se allo arxeio
		public static final int PAGE_SIZE = 4096;
		public static final long FILE_SIZE = PAGE_SIZE*2000L;
		public static final byte[] BLANK_PAGE = new byte[PAGE_SIZE];
		public static final String FILE_NAME = "test.txt";		
		
		public Page(int pageid, byte[] data) {
			this.pageid = pageid;
			this.data = ByteBuffer.wrap(data);
		}
		
		public ByteBuffer getData() {
			return data;
		}
		
		public int getPageId() {
			return pageid;
		}
		
		public void setPageId(int pageid) {
			this.pageid = pageid;
		}
		
		public void setDirty(){
			dirty = true;
		}
		
		public byte readByte(int pos) {
			return data.get(pos);
		}
		
		public int readInt(int pos) {
			return data.getInt(pos);
		}
		
		/*public String readString(){
			//TODO
		}*/
		
		public void writeByte(int pos, byte value) {
			setDirty();
			data.put(pos, value);
		}
		
		public void writeInt(int pos, int value) {
			setDirty();
			data.putInt(pos, value);
		}
		
		
		
		public static void main(String args[]) throws Exception {
		
				Page obj = new Page(0, BLANK_PAGE);
				obj.setDirty();
				System.out.println("The page id is "+obj.pageid);
				System.out.println("The dirty field is "+obj.dirty);
				ByteBuffer buf = obj.getData();
				obj.writeInt(0,5);
				boolean a = buf.hasArray();
				System.out.println("The buffer is backed by an array? " + a);				
				System.out.println(buf.getInt());				
				preallocateTestFile(FILE_NAME);				
		}
	
		//makes an empty file by writing blank pages of 4096 size
		private static void preallocateTestFile(final String fileName) throws Exception {
			
			RandomAccessFile file = new RandomAccessFile(fileName, "rw");
			for (long i = 0; i< FILE_SIZE; i += PAGE_SIZE){
			
				file.write(BLANK_PAGE, 0, PAGE_SIZE);
			}
			
			file.close();
		}			
}

