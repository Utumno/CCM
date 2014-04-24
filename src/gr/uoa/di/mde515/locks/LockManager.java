package gr.uoa.di.mde515.locks;

import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.index.PageId;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager<T> {

	public final static class Request<T> {

		PageId<T> pageId;
		Transaction tr;
		Lock lock;

		public Request(PageId<T> pageId, Transaction tr, Lock lock) {
			this.pageId = pageId;
			this.tr = tr;
			this.lock = lock;
		}
	}

	private final Map<PageId<T>, List<Request<T>>> locks = new ConcurrentHashMap<>();

	public Map<PageId<T>, List<Request<T>>> getLocks() {
		return locks;
	}
}
