package gr.uoa.di.mde515.engine.buffer;

import java.nio.ByteBuffer;

/**
 * Interface exporting an API for serializing and deserializing an arbitrary
 * type to a ByteBuffer. It is meant to be implemented by stateless, one element
 * enums - so its implementations are immutable. Maybe should create/consume
 * byte[] ? <br/>
 * Implementation note: offsets ideally would be shorts but are ints (see <a
 * href="http://stackoverflow.com/a/477776/281545">this</a>).
 *
 * @param <V>
 *            the type to be serialized and deserialized
 */
public interface Serializer<V> {

	V readValue(ByteBuffer dat, int offset);

	void writeValue(ByteBuffer dat, int offset, V value);

	short getTypeSize(); // FIXME bin ?
}
