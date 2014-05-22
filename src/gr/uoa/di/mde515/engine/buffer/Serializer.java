package gr.uoa.di.mde515.engine.buffer;

import java.nio.ByteBuffer;

/**
 * WIP - had to change the signature to int from short cause of <a
 * href="http://stackoverflow.com/a/477776/281545">this</a>
 */
public interface Serializer<K extends Comparable<K>, V> {

	K readKey(ByteBuffer dat, int slot, short header_size);

	V readValue(ByteBuffer dat, int slot, short header_size);

	void writeKey(ByteBuffer dat, int slot, K key, short header_size);

	void writeValue(ByteBuffer dat, int slot, V value, short header_size);

	short getKeySize(); // FIXME bin

	short getRecordSize(); // FIXME bin
}
