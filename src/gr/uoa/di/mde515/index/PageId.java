package gr.uoa.di.mde515.index;

public class PageId<T> {

	T id;

	public synchronized T getId() {
		return id;
	}

	public PageId(T id) {
		this.id = id;
	}
}
