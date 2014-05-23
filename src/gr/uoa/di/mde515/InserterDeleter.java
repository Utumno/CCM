package gr.uoa.di.mde515;

import gr.uoa.di.mde515.engine.Engine;
import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.index.Record;
import gr.uoa.di.mde515.locks.DBLock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

final class InserterDeleter<T> implements Callable<T> {

	private final Engine<Integer, Integer, Integer> eng;
	private final Record<Integer, Integer> rec;

	InserterDeleter(Engine<Integer, Integer, Integer> eng,
			Record<Integer, Integer> rec) {
		this.eng = eng;
		this.rec = rec;
	}

	@Override
	public T call() throws Exception {
		Transaction tr = eng.beginTransaction();
		List<Integer> primeNumbers = new ArrayList<>(Arrays.asList(2, 3, 5, 7,
			11, 13, 17, 19, 23, 29, 31, 41, 43, 47, 53, 61, 59, 67, 71, 73, 79,
			83, 89, 97, 101, 103, 109, 113, 127, 131, 137, 139, 149, 151, 157,
			163, 167, 173, 179, 107));
		// for (int j = 0; ++j < 10;) {
		List<Integer> perm = Main.permutation(primeNumbers);
		System.out.println(perm);
		try {
			// final int RECORDS = 7;
			// for (int in = 0; in < RECORDS; ++in) {
			for (Integer in : perm) {
				System.out.println("--------------INSERT " + in);
				eng.insert(tr, new Record<>(in, in));
				eng.print(tr, DBLock.E);
				// Thread.sleep(300);
			}
			perm = Main.permutation(primeNumbers);
			System.out.println(perm);
			// for (int in = RECORDS - 1; in >= 0; --in) {
			for (Integer in : perm) {
				System.out.println("--------------LOOKUP " + in);
				Record<Integer, Integer> rkv = eng.lookup(tr, in, DBLock.E);
				System.out.println("The record is "
					+ (rkv == null ? null
							: ("key " + rkv.getKey() + "  value " + rkv
								.getValue())) + " -- " + tr);
				// Thread.sleep(100);
			}
			eng.print(tr, DBLock.E);
			// Thread.sleep(500);
			System.out.println("DELETING");
			// for (int in = 0; in < RECORDS; ++in) {
			perm = Main.permutation(primeNumbers);
			System.out.println(perm);
			// for (int in = RECORDS - 1; in >= 0; --in) {
			for (Integer in : perm) {
				System.out.println("--------------DELETING " + in);
				eng.delete(tr, in, DBLock.E);
				eng.print(tr, DBLock.E);
				// Thread.sleep(100);
			}
			eng.print(tr, DBLock.E); // must be -1::0
			// Thread.sleep(2000);
			eng.commit(tr);
		} finally {
			eng.endTransaction(tr);
		}
		return null;
	}
}
