package gr.uoa.di.mde515;

import gr.uoa.di.mde515.engine.Engine;
import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.index.Record;
import gr.uoa.di.mde515.locks.DBLock;

import java.util.concurrent.Callable;

final class Lookuper<T> implements Callable<T> {

	private final Engine<Integer, Integer, Integer> eng;
	private final Integer key;

	Lookuper(Engine<Integer, Integer, Integer> eng, Integer key) {
		this.eng = eng;
		this.key = key;
	}

	@Override
	public T call() throws Exception {
		Transaction tr = eng.beginTransaction();
		try {
			Record<Integer, Integer> rkv = eng.lookup(tr, key, DBLock.S);
			System.out
				.println("The record is "
					+ (rkv == null ? null
							: ("key " + rkv.getKey() + "  value " + rkv
								.getValue())));
		} finally {
			eng.endTransaction(tr);
		}
		return null;
	}
}
