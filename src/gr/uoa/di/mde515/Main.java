package gr.uoa.di.mde515;

import gr.uoa.di.mde515.engine.CCM.TransactionRequiredException;
import gr.uoa.di.mde515.engine.Engine;
import gr.uoa.di.mde515.engine.Engine.TransactionFailedException;
import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.index.KeyExistsException;
import gr.uoa.di.mde515.index.Record;
import gr.uoa.di.mde515.locks.DBLock;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {

	private static Random r = new Random();
	// THREADS //
	private static final int NUM_OF_THREADS = Runtime.getRuntime()
		.availableProcessors();
	private static final ExecutorService exec = Executors
		.newFixedThreadPool(NUM_OF_THREADS);

	public static <T> void main(String[] args) throws InterruptedException,
			IOException {
		final Engine<Integer, Integer, Integer> eng = Engine.newInstance();
		try {
			// treePrint(eng);
			for (int i = 0; i < 100; ++i)
				exec.submit(new Inserter<T>(eng, new Record<>(i, i))).get();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			exec.shutdown();
			boolean terminated = exec.awaitTermination(13, TimeUnit.SECONDS);
			// if timed out terminated will be false
			// if (!terminated) {
			// List<Runnable> notExecuted = INSTANCE.exec.shutdownNow();
			// }
			eng.shutdown();
		}
	}

	private static void treePrint(Engine eng) throws FileNotFoundException,
			IOException, InterruptedException, TransactionRequiredException,
			KeyExistsException, TransactionFailedException {
		List<Integer> primeNumbers = new ArrayList<>(Arrays.asList(2, 3, 5, 7,
			11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 61, 59, 67, 71, 73,
			79, 83, 89, 97, 101, 103, 109, 113, 127, 131, 137, 139, 149, 151,
			157, 163, 167, 173, 179, 107));
		// for (int j = 0; ++j < 10;) {
		List<Integer> perm = permutation(primeNumbers);
		System.out.println(perm);
		Transaction tr = eng.beginTransaction();
		for (int i = 0; i < perm.size(); i++) {
			final Integer in = perm.get(i);
			Record<Integer, Integer> rec = new Record<>(in, in);
			eng.insertIndex(tr, rec);
			Thread.sleep(300);
			// bPlusTree.print(tr, DBLock.E);
		}
		eng.print(tr, DBLock.E);
		System.out.println();
		eng.commit(tr);
		// }
	}

	static List<Integer> permutation(List<Integer> primeNumbers) {
		primeNumbers = new ArrayList<>(primeNumbers); // NEEDED
		final List<Integer> perm = new ArrayList<>();
		for (int inLen = primeNumbers.size(); inLen != 0; --inLen) {
			int nextInt = r.nextInt(inLen);
			perm.add(primeNumbers.remove(nextInt));
		}
		return perm;
	}

	private static final class Inserter<T> implements Callable<T> {

		private final Engine<Integer, Integer, Integer> eng;
		private final Record<Integer, Integer> rec;

		Inserter(Engine<Integer, Integer, Integer> eng,
				Record<Integer, Integer> rec) {
			this.eng = eng;
			this.rec = rec;
		}

		@Override
		public T call() throws Exception {
			Transaction tr = eng.beginTransaction();
			try {
				// for (int i = 0; i < 5; ++i)
				eng.insert(tr, rec);
				eng.commit(tr);
				// eng.print(tr, DBLock.E);
			} finally {
				eng.endTransaction(tr);
			}
			return null;
		}
	}

	private static final class Lookuper<T> implements Callable<T> {

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
				Record<Integer, Integer> rkv = eng.lookup(tr, key, DBLock.E);
			} finally {
				eng.endTransaction(tr);
			}
			return null;
		}
	}
}
