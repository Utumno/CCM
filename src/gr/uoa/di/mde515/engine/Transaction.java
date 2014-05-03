package gr.uoa.di.mde515.engine;

import java.util.concurrent.atomic.AtomicLong;

public class Transaction {

	private static final AtomicLong transactionId;
	private final long threadId; // not really needed
	private final String threadName;
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

	public void flush() {
		throw new UnsupportedOperationException("Not implemented"); // TODO
	}
}
