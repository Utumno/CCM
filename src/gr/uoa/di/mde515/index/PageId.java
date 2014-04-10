package gr.uoa.di.mde515.index;

public class PageId<T> {

	T o;

	public synchronized T getO() {
		return o;
	}

	public PageId(T o) {
		this.o = o;
	}
}
