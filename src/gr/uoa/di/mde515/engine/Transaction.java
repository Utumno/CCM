package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.files.DataFile;
import gr.uoa.di.mde515.index.Index;
import gr.uoa.di.mde515.index.PageId;
import gr.uoa.di.mde515.locks.DBLock;
import gr.uoa.di.mde515.locks.LockManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

public class Transaction {

	private static final AtomicLong transactionId;
	private static final LockManager lm = LockManager.getInstance();
	private final long threadId; // not really needed FIXME
	private final String threadName;
	private final EnumMap<DBLock, List<PageId<Integer>>> lockedDataPages = new EnumMap<>(
		DBLock.class);
	private final EnumMap<DBLock, List<PageId<Integer>>> lockedIndexPages = new EnumMap<>(
		DBLock.class);
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
		lockedDataPages.put(DBLock.E, new ArrayList<PageId<Integer>>());
		lockedDataPages.put(DBLock.S, new ArrayList<PageId<Integer>>());
		lockedIndexPages.put(DBLock.E, new ArrayList<PageId<Integer>>());
		lockedIndexPages.put(DBLock.S, new ArrayList<PageId<Integer>>());
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
		for (List<PageId<Integer>> list : lockedDataPages.values())
			dataFile.flush(list);
	}

	public <K extends Comparable<K>> void flushIndex(final Index<K, ?> index)
			throws IOException {
		for (List<PageId<Integer>> list : lockedIndexPages.values())
			index.flush(list);
	}

	public <K extends Comparable<K>, V> void
			abort(final DataFile<K, V> dataFile) throws IOException {
		for (List<PageId<Integer>> list : lockedDataPages.values())
			dataFile.abort(list);
	}

	public void addLockedDataPage(PageId<Integer> pageId, DBLock lock) {
		if (!(pageId.getId() instanceof Integer)) return; // FIXME horrible hack
		// - actually the pageId may wrap a node at this stage
		System.out.println("ADDED " + pageId.getId());
		lockedDataPages.get(lock).add(pageId);
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
		for (Entry<DBLock, List<PageId<Integer>>> entries : lockedDataPages
			.entrySet()) {
			if (entries.getKey() != lock && entries.getValue().contains(pid))
				throw new IllegalStateException("You already hold a "
					+ entries.getKey() + " lock for page " + pageID
					+ ". You can't acquire a " + lock + " lock.");
		}
		if (!lockedDataPages.get(lock).contains(pid)) {
			lm.requestLock(new LockManager.Request(pid, this, lock));
			addLockedDataPage(pid, lock);
			return true;
		}
		return false;
	}

	public String getThreadId() {
		return threadName;
	}
}
