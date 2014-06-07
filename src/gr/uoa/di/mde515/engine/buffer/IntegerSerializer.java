package gr.uoa.di.mde515.engine.buffer;

import java.nio.ByteBuffer;

// TODO is autoboxing here a performance nono ?
public class IntegerSerializer implements Serializer<Integer> {

	private static final short type_size = 4;

	@Override
	public Integer readValue(ByteBuffer dat, int offset) {
		return dat.getInt(offset);
	}

	@Override
	public void writeValue(ByteBuffer dat, int offset, Integer value) {
		dat.putInt(offset, value);
	}

	@Override
	public short getTypeSize() {
		return type_size;
	}
}
