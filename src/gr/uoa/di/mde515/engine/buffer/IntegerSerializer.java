package gr.uoa.di.mde515.engine.buffer;

import java.nio.ByteBuffer;

/**
 * Enum to read and write Integers to/from a byte buffer. Immutable - should be
 * an enum - TODO. Is autoboxing here a performance nono ?
 *
 */
public enum IntegerSerializer implements Serializer<Integer> {
	INSTANCE;
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
