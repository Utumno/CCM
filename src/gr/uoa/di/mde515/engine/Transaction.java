package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.files.DataFile;
import gr.uoa.di.mde515.index.Index;
import gr.uoa.di.mde515.locks.DBLock;
import gr.uoa.di.mde515.locks.LockManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

public final class Transaction {

	private static final AtomicLong transactionId;
	private static final LockManager lm = LockManager.getInstance();
	private final long threadId; // not really needed FIXME
	private final String threadName;
	// TODO merge below two maps
	private final EnumMap<DBLock, List<Integer>> lockedDataPages = new EnumMap<>(
		DBLock.class);
	private final EnumMap<DBLock, List<Integer>> lockedIndexPages = new EnumMap<>(
		DBLock.class);
	private final long transId; // TODO Random unique trans identifier? see:
	// http://www.javapractices.com/topic/TopicAction.do?Id=56
	// http://bugs.java.com/view_bug.do?bug_id=6611830
	// http://stackoverflow.com/questions/7212635/is-java-util-uuid-thread-safe
	private volatile State state;
	static {
		transactionId = new AtomicLong();
	}

	/* package private */Transaction() {
		transId = transactionId.incrementAndGet();
		threadId = Thread.currentThread().getId();
		threadName = "Thread [" + threadId + "] for transaction " + transId;
		Thread.currentThread().setName(threadName);
		lockedDataPages.put(DBLock.E, new ArrayList<Integer>());
		lockedDataPages.put(DBLock.S, new ArrayList<Integer>());
		lockedIndexPages.put(DBLock.E, new ArrayList<Integer>());
		lockedIndexPages.put(DBLock.S, new ArrayList<Integer>());
		state = State.ACTIVE;
		System.out.println(this + " INITIALIZED");
	}

	// =========================================================================
	// API
	// =========================================================================
	/**
	 * Tries to lock a page if not already locked. May block. Returns true if
	 * the page was locked for the first time by this transaction or false if
	 * the page was already locked. Used to update the pin count on the page
	 * (one pin per transaction).
	 *
	 * @param pageID
	 *            the pageId to be locked
	 * @param lock
	 *            the type of lock
	 * @return true if the page was locked for the first time by this
	 *         transaction, false otherwise
	 * @throws InterruptedException
	 *             if the thread is interrupted while blocked waiting for the
	 *             lock
	 */
	public boolean lock(int pageID, DBLock lock) throws InterruptedException {
		state = state.transition(State.ACTIVE);
		for (Entry<DBLock, List<Integer>> entries : lockedDataPages.entrySet()) {
			if (entries.getKey() != lock && entries.getValue().contains(pageID))
				throw new IllegalStateException("You already hold a "
					+ entries.getKey() + " lock for page " + pageID
					+ ". You can't acquire a " + lock + " lock.");
		}
		for (Entry<DBLock, List<Integer>> entries : lockedIndexPages.entrySet()) {
			if (entries.getKey() != lock && entries.getValue().contains(pageID))
				throw new IllegalStateException("You already hold a "
					+ entries.getKey() + " lock for page " + pageID
					+ ". You can't acquire a " + lock + " lock.");
		}
		if (!lockedDataPages.get(lock).contains(pageID)
			&& !lockedIndexPages.get(lock).contains(pageID)) {
			lm.requestLock(new LockManager.Request(pageID, this, lock));
			addLockedDataPage(pageID, lock);
			return true;
		}
		return false;
	}

	// =========================================================================
	// Package private
	// =========================================================================
	void validateThread() {
		final long id = Thread.currentThread().getId();
		final String name = Thread.currentThread().getName();
		if (id != threadId || !name.equals(threadName)) {
			throw new IllegalStateException("Calling thread (" + id + ": "
				+ name + ") is not owner of transaction with id " + transId);
		}
	}

	<K extends Comparable<K>, V> void commit(final DataFile<K, V> dataFile,
			final Index<K, ?> index) throws IOException {
		System.out.println(this + " flushing " + lockedDataPages
			+ lockedIndexPages);
		state = state.transition(State.COMMITING);
		for (List<Integer> list : lockedDataPages.values())
			dataFile.flush(list);
		for (List<Integer> list : lockedIndexPages.values())
			index.flush(list);
	}

	<K extends Comparable<K>, V> void abort(final DataFile<K, V> dataFile,
			final Index<K, ?> index) throws IOException {
		System.out.println(this + " aborting " + lockedDataPages
			+ lockedIndexPages);
		state = state.transition(State.ABORTING);
		for (List<Integer> list : lockedDataPages.values())
			dataFile.abort(list);
		for (List<Integer> list : lockedIndexPages.values())
			index.abort(list);
	}

	void end() {
		state = state.transition(State.ENDING);
		for (Entry<DBLock, List<Integer>> entries : lockedDataPages.entrySet()) {
			for (int pid : entries.getValue()) {
				lm.unlock(this, pid);
			}
		}
		for (Entry<DBLock, List<Integer>> entries : lockedIndexPages.entrySet()) {
			for (int pid : entries.getValue()) {
				lm.unlock(this, pid);
			}
		}
	}

	// =========================================================================
	// Helpers
	// =========================================================================
	private void addLockedDataPage(int pageId, DBLock lock) {
		// if (!(pageId.getId() instanceof Integer)) return; // FIXME horrible
		// hack - actually the pageId may wrap a node at this stage
		// System.out.println("ADDED " + id);
		if (pageId >= 0) lockedDataPages.get(lock).add(pageId); // FIXME hack2
		// I should have one map for each file and pass it as param to add
		else lockedIndexPages.get(lock).add(pageId);
	}

	private enum State {
		ACTIVE, COMMITING, ABORTING, ENDING;

		/**
		 * Meant to allow ACTIVE > COMMIT (once) or ABORT (many) > ENDING.
		 *
		 * @param next
		 *            the next state
		 * @throws IllegalTransitionException
		 *             if above does not hold
		 */
		State transition(State next) throws IllegalTransitionException {
			switch (this) {
			case ACTIVE:
				if (next == ENDING)
					throw new IllegalTransitionException(this + " to " + next);
				return next;
			case COMMITING:
				if (next == ACTIVE || next == this) // call commit once
					throw new IllegalTransitionException(this + " to " + next);
				return next; // can call abort from commit (commit failed)
			case ABORTING:
				if (next == ACTIVE || next == COMMITING)
					throw new IllegalTransitionException(this + " to " + next);
				return next;
			default:
				throw new IllegalTransitionException(this + " to " + next);
			}
		}

		/** TODO make into checked exception */
		final class IllegalTransitionException extends RuntimeException {

			private static final long serialVersionUID = -2944616043222631837L;

			public IllegalTransitionException(String string) {
				super(string);
			}
		}
	}

	// =========================================================================
	// Object methods
	// =========================================================================
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Transaction [id=");
		builder.append(transId);
		builder.append("]");
		return builder.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (transId ^ (transId >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		Transaction other = (Transaction) obj;
		if (transId != other.transId) return false;
		return true;
	}
}
