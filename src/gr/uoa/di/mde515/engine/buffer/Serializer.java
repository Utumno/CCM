package gr.uoa.di.mde515.engine.buffer;

import java.nio.ByteBuffer;


public interface Serializer<K extends Comparable<K>, V> {

	K readKey(ByteBuffer dat, int offset);

	V readValue(ByteBuffer dat, int offset);

	void writeKey(ByteBuffer dat, int offset, K key);

	void writeValue(ByteBuffer dat, int offset, V value);
}
