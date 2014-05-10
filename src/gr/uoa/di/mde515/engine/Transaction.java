package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.files.DataFile;
import gr.uoa.di.mde515.index.PageId;
import gr.uoa.di.mde515.locks.DBLock;
import gr.uoa.di.mde515.locks.LockManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Transaction {

	private static final AtomicLong transactionId;
	private static final LockManager lm = LockManager.getInstance();
	private final long threadId; // not really needed
	private final String threadName;
	private final List<PageId<Integer>> lockedDataPages = new ArrayList<>();
	// TODO Random unique trans identifier added to thread name
	// http://www.javapractices.com/topic/TopicAction.do?Id=56
	// http://bugs.java.com/view_bug.do?bug_id=6611830
	// http://stackoverflow.com/questions/7212635/is-java-util-uuid-thread-safe
	static {
		transactionId = new AtomicLong();
	}

	public Transaction() {
		long transId = transactionId.incrementAndGet();
		threadId = Thread.currentThread().getId();
		threadName = "Thread (" + threadId + ") for transaction " + transId;
		Thread.currentThread().setName(threadName);
	}

	public void validateThread() {
		final long id = Thread.currentThread().getId();
		final String name = Thread.currentThread().getName();
		if (id != threadId || !name.equals(threadName)) {
			throw new IllegalStateException("Calling thread (" + id + ": "
				+ name + ") is not owner of transaction with id " + threadId);
		}
	}

	public <K extends Comparable<K>, V> void
			flush(final DataFile<K, V> dataFile) throws IOException {
		dataFile.flush(lockedDataPages);
	}

	public void addLockedDataPage(PageId<Integer> pageId) {
		if (!(pageId.getId() instanceof Integer)) return; // FIXME horrible hack
		// - actually the pageId may wrap a node at this stage
		System.out.println("ADDED " + pageId.getId());
		lockedDataPages.add(pageId);
	}

	/**
	 * Tries to lock a page if not already locked. May block. Returns true if
	 * the page was locked or false if the page was already locked.
	 *
	 * @param pageID
	 *            the pageId to be locked
	 * @param lock
	 *            the type of lock
	 * @return true if the page was locked for the first time by this
	 *         transaction, false otherwise
	 */
	public boolean lock(int pageID, DBLock lock) {
		final PageId<Integer> pid = new PageId<>(pageID);
		if (!this.lockedDataPages.contains(pid)) {
			lm.requestLock(new LockManager.Request(pid, this,
				lock));
			addLockedDataPage(pid);
			return true;
		}
		return false;
	}
}
