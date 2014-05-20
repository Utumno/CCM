package gr.uoa.di.mde515;

import gr.uoa.di.mde515.engine.Engine;
import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.index.Record;
import gr.uoa.di.mde515.locks.DBLock;

import java.util.concurrent.Callable;

class Inserter<T> implements Callable<T> {

	private final Engine<Integer, Integer, Integer> eng;
	private final Record<Integer, Integer> rec;

	Inserter(Engine<Integer, Integer, Integer> eng, Record<Integer, Integer> rec) {
		this.eng = eng;
		this.rec = rec;
	}

	@Override
	public T call() throws Exception {
		Transaction tr = eng.beginTransaction();
		try {
			Record<Integer, Integer> rkv = eng.lookup(tr, rec.getKey(),
				DBLock.E);
			System.out
				.println("The record is "
					+ (rkv == null ? null
							: ("key " + rkv.getKey() + "  value " + rkv
								.getValue())));
			if (rkv == null) {
				eng.insert(tr, rec);
			} else {
				System.out.println("THE RECORD already exists. NOT inserting");
			}
			eng.commit(tr);
			// eng.print(tr, DBLock.E);
		} finally {
			eng.endTransaction(tr);
		}
		return null;
	}
}
