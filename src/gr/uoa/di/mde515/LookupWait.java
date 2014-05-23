package gr.uoa.di.mde515;

import gr.uoa.di.mde515.engine.Engine;
import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.index.Record;
import gr.uoa.di.mde515.locks.DBLock;

import java.util.Random;
import java.util.concurrent.Callable;

final class LookupWait<T> implements Callable<T> {

	private final Engine<Integer, Integer, Integer> eng;
	private final Integer key;
	private static Random r = new Random();

	LookupWait(Engine<Integer, Integer, Integer> eng, Integer key) {
		this.eng = eng;
		this.key = key;
	}

	@Override
	public T call() throws Exception {
		Transaction tr = eng.beginTransaction();
		try {
			long time = r.nextInt(1000);
			System.out.println("The random is " + time);
			eng.waitTransaction(time);
			Record<Integer, Integer> rkv = eng.lookup(tr, key, DBLock.S);
			System.out
				.println("The record is "
					+ (rkv == null ? null
							: ("key " + rkv.getKey() + "  value " + rkv
								.getValue())) + " -- " + tr);
		} finally {
			eng.endTransaction(tr);
		}
		return null;
	}
}
