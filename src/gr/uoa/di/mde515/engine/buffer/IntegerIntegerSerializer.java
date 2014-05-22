package gr.uoa.di.mde515.engine.buffer;

import java.nio.ByteBuffer;

// TODO is autoboxing here a performance nono ?
public class IntegerIntegerSerializer implements Serializer<Integer, Integer> {

	@Override
	public Integer readKey(ByteBuffer dat, int offset) {
		return dat.getInt(offset);
	}

	@Override
	public Integer readValue(ByteBuffer dat, int offset) {
		return dat.getInt(offset);
	}

	@Override
	public void writeKey(ByteBuffer dat, int offset, Integer key) {
		dat.putInt(offset, key);
	}

	@Override
	public void writeValue(ByteBuffer dat, int offset, Integer value) {
		dat.putInt(offset, value);
	}
}
