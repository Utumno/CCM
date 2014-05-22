package gr.uoa.di.mde515.engine.buffer;

import java.nio.ByteBuffer;

// TODO is autoboxing here a performance nono ?
public class IntegerIntegerSerializer implements Serializer<Integer, Integer> {

	private static final short key_size = 4;
	private static final short value_size = 4;
	private static final short record_size = key_size + value_size;

	@Override
	public Integer readKey(ByteBuffer dat, int slot, short header_size) {
		return dat.getInt(header_size + slot * record_size);
	}

	@Override
	public Integer readValue(ByteBuffer dat, int slot, short header_size) {
		return dat.getInt(header_size + slot * record_size + key_size);
	}

	@Override
	public void writeKey(ByteBuffer dat, int slot, Integer key,
			short header_size) {
		dat.putInt(header_size + slot * record_size, key);
	}

	@Override
	public void writeValue(ByteBuffer dat, int slot, Integer value,
			short header_size) {
		dat.putInt(header_size + slot * record_size + key_size, value);
	}

	@Override
	public short getKeySize() {
		return key_size;
	}

	@Override
	public short getRecordSize() {
		return record_size;
	}
}
