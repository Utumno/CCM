package gr.uoa.di.mde515.locks;

import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.index.PageId;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockManager {

	public final static class Request {

		PageId<?> pageId;
		Transaction tr;
		DBLock lock;

		public Request(PageId<?> pageId, Transaction tr, DBLock lock) {
			this.pageId = pageId;
			this.tr = tr;
			this.lock = lock;
		}
	}

	private LockManager() {}

	private static final ConcurrentMap<PageId<?>, LockStructure> locks = new ConcurrentHashMap<>();
	private static final LockManager instance = new LockManager();

	public Map<PageId<?>, LockStructure> getLocks() {
		return locks;
	}

	public static LockManager getInstance() {
		return instance;
	}

	public void requestLock(Request request) {
		LockStructure pageLock = null;
		synchronized (locks) {
			pageLock = locks.get(request.pageId);
			if (pageLock == null) {
				pageLock = new LockStructure();
				locks.put(request.pageId, pageLock);
			}
			// make sure that no other thread deletes the pageLock by adding to
			// trans
			pageLock.add(request);
		}
		pageLock.lock(request.lock); // this may block so must be out of the
		// synchronized block
	}

	private final static class LockStructure {

		/** A semi fair lock - FIFO with preference to writers */
		private final ReadWriteLock rw = new ReentrantReadWriteLock(true);
		private final Lock r = rw.readLock();
		private final Lock w = rw.writeLock();
		// private volatile boolean inUse = true; // in use == !trans.isEmpty()
		private final LinkedHashMap<Transaction, DBLock> trans = new LinkedHashMap<>();

		LockStructure() {}

		void lock(DBLock lock) {
			switch (lock) {
			case E:
				w.lock();
				return;
			case S:
				r.lock();
				return;
			}
			throw new RuntimeException("Forgotten enum constant");
		}

		synchronized void add(Request request) {
			trans.put(request.tr, request.lock);
		}
	}
}
