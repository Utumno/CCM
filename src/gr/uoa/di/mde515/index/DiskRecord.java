package gr.uoa.di.mde515.index;

public abstract class DiskRecord<K extends Comparable<K>, V> extends
		Record<K, V> {

	public DiskRecord(K key, V value) {
		super(key, value);
	}

	public abstract K getKeyFromBytes(byte[] b);

	abstract V getValueFromBytes(byte[] b);
}
