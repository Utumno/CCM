package gr.uoa.di.mde515.index;

public class Record<K extends Comparable<K>, V> {

	private K key;
	private V value;

	public Record(K key, V value) {
		this.key = key;
		this.value = value;
	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public V getValue() {
		return value;
	}

	public void setValue(V value) {
		this.value = value;
	}
}