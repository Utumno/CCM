package gr.uoa.di.mde515.index;

/**
 * Essentially a Pair implementation with the only restriction that the key
 * ("first") should be comparable. If K and V are immutable this is immutable.
 * TODO: move to a "helpers" package
 *
 * @param <K>
 *            the key type
 * @param <V>
 *            the value type
 */
public class Record<K extends Comparable<K>, V> {

	private final K key;
	private final V value;

	public Record(K key, V value) {
		this.key = key;
		this.value = value;
	}

	public K getKey() {
		return key;
	}

	public V getValue() {
		return value;
	}

	// =========================================================================
	// Object Overrides
	// =========================================================================
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		Record other = (Record) obj;
		if (key == null) {
			if (other.key != null) return false;
		} else if (!key.equals(other.key)) return false;
		if (value == null) {
			if (other.value != null) return false;
		} else if (!value.equals(other.value)) return false;
		return true;
	}

	@Override
	public String toString() {
		return "[" + key + ";" + value + "]";
	}
}
