package gr.uoa.di.mde515.locks;

import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.index.PageId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LockManager<T> {

	public final static class Request<T> {

		public Request(PageId<T> pageId, Transaction tr, Lock lock) {
			super();
			this.pageId = pageId;
			this.tr = tr;
			this.lock = lock;
		}
		PageId<T> pageId;
		Transaction tr;
		Lock lock;
	}

	private final Map<PageId<T>, List<Request<T>>> locks = new HashMap<>();

	public Map<PageId<T>, List<Request<T>>> getLocks() {
		return locks;
	}
}
