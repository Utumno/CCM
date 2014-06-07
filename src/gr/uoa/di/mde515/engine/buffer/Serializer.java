package gr.uoa.di.mde515.engine.buffer;

import java.nio.ByteBuffer;

/**
 * WIP - had to change the signature to int from short cause of <a
 * href="http://stackoverflow.com/a/477776/281545">this</a>
 */
public interface Serializer<V> {

	V readValue(ByteBuffer dat, int offset);

	void writeValue(ByteBuffer dat, int offset, V value);

	short getTypeSize(); // FIXME bin
}
