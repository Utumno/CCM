package gr.uoa.di.mde515.locks;

import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.index.PageId;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockManager {

	private LockManager() {}

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

	private static final ConcurrentMap<PageId<?>, LockStructure> locks = new ConcurrentHashMap<>();
	private static final LockManager instance = new LockManager();

	public Map<PageId<?>, LockStructure> getLocks() {
		return locks;
	}

	public static LockManager getInstance() {
		return instance;
	}

	public void requestLock(Request request) {
		LockStructure lockStruct = null;
		synchronized (locks) {
			lockStruct = locks.get(request.pageId);
			if (lockStruct == null) {
				lockStruct = new LockStructure();
				locks.put(request.pageId, lockStruct);
			}
			// make sure that no other thread deletes the pageLock by adding to
			// trans
			lockStruct.add(request);
		}
		lockStruct.lock(request); // this may block so must be
		// out of the synchronized block
	}

	private final static class LockStructure {

		/** A semi fair lock - FIFO with preference to writers */
		private final ReadWriteLock rw = new ReentrantReadWriteLock(true);
		private final Lock r = rw.readLock();
		private final Lock w = rw.writeLock();
		// private volatile boolean inUse = true; // in use == !trans.isEmpty()
		// FIXME excessive synchronization
		private final Map<Transaction, DBLock> requests = Collections
			.synchronizedMap(new LinkedHashMap<Transaction, DBLock>());
		private final Map<Transaction, Request> granted = Collections // FIXME
			.synchronizedMap(new LinkedHashMap<Transaction, Request>());

		LockStructure() {}

		void lock(Request req) {
			final DBLock lock = req.lock;
			final Transaction trans = req.tr;
			final Request grant = granted.get(trans);
			if (grant != null && grant.pageId.equals(req.pageId)
				&& grant.lock == lock) return; // maybe reduntant checks ?
			switch (lock) {
			case E:
				w.lock();
				break;
			case S:
				r.lock();
				break;
			}
			granted.put(trans, req);
		}

		synchronized void add(Request request) {
			requests.put(request.tr, request.lock);
		}
	}
}
