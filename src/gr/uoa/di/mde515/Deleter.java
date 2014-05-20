package gr.uoa.di.mde515;

import gr.uoa.di.mde515.engine.Engine;
import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.index.PageId;
import gr.uoa.di.mde515.locks.DBLock;

import java.util.concurrent.Callable;

final class Deleter<T> implements Callable<T> {

	private final Engine<Integer, Integer, Integer> eng;
	private final Integer key;

	Deleter(Engine<Integer, Integer, Integer> eng, Integer key) {
		this.eng = eng;
		this.key = key;
	}

	@Override
	public T call() throws Exception {
		Transaction tr = eng.beginTransaction();
		try {
			eng.delete(tr, key, DBLock.E, new PageId<Integer>(2));
			System.out.println("DELETED");
			eng.commit(tr);
		} finally {
			eng.endTransaction(tr);
		}
		return null;
	}
}
