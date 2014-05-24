package gr.uoa.di.mde515.index;

/**
 * A generic page id - defines the equals and hash code methods and wraps an id
 * of type T. Must be immutable - will be if T is immutable. If I verify
 * immutability I can make equals just check the instance with ==
 *
 * @param <T>
 *            the type of the page id - must be immutable
 */
public final class PageId<T> {

	private final T id;

	public synchronized T getId() { // TODO bin
		return id;
	}

	public PageId(T id) {
		this.id = id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		PageId other = (PageId) obj;
		if (id == null) {
			if (other.id != null) return false;
		} else if (!id.equals(other.id)) return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("PageId [id=");
		builder.append(id);
		builder.append("]");
		return builder.toString();
	}
}
