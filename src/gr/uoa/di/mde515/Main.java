package gr.uoa.di.mde515;

import gr.uoa.di.mde515.engine.CCM.TransactionRequiredException;
import gr.uoa.di.mde515.engine.Engine;
import gr.uoa.di.mde515.engine.Engine.TransactionFailedException;
import gr.uoa.di.mde515.engine.Engine.TransactionalOperation;
import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.engine.buffer.IntegerIntegerSerializer;
import gr.uoa.di.mde515.index.KeyExistsException;
import gr.uoa.di.mde515.index.Record;
import gr.uoa.di.mde515.locks.DBLock;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class Main {

	private static Random r = new Random();

	public static <T> void main(String[] args) throws InterruptedException,
			IOException {
		final Engine<Integer, Integer, Integer> eng = (Engine<Integer, Integer, Integer>) Engine
			.newInstance(new IntegerIntegerSerializer());// FIXME unchecked and
															// ugly
		try {
			final Record<Integer, Integer> rec = new Record<>(1000, 1000);
			TransactionalOperation to = eng.new TransactionalOperation() {

				@Override
				public void execute() throws IOException, InterruptedException,
						TransactionRequiredException, KeyExistsException,
						TransactionFailedException {
					Record<Integer, Integer> rkv = lookup(rec.getKey(),
						DBLock.E);
					System.out.println("The record is "
						+ (rkv == null ? null : ("key " + rkv.getKey()
							+ "  value " + rkv.getValue())));
					if (rkv == null) {
						insert(rec);
					} else {
						System.out.println("THE RECORD already exists. "
							+ "NOT inserting");
					}
					commit();
					// eng.print(tr, DBLock.E);
				}
			};
			eng.submit(to);
			to = eng.new TransactionalOperation() {

				@Override
				public void execute() throws IOException, InterruptedException,
						TransactionRequiredException, KeyExistsException,
						TransactionFailedException, ExecutionException {
					List<Integer> primeNumbers = new ArrayList<>(Arrays.asList(
						2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47,
						53, 61, 59, 67, 71, 73, 79, 83, 89, 97, 101, 103, 109,
						113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173,
						179, 107));
					// for (int j = 0; ++j < 10;) {
					List<Integer> perm = Main.permutation(primeNumbers);
					System.out.println(perm);
					// final int RECORDS = 7;
					// for (int in = 0; in < RECORDS; ++in) {
					for (Integer in : perm) {
						System.out.println("--------------INSERT " + in);
						insert(new Record<>(in, in));
						print(DBLock.E);
						// Thread.sleep(300);
					}
					print(DBLock.E);
					// Thread.sleep(500);
					System.out.println("DELETING");
					// for (int in = 0; in < RECORDS; ++in) {
					perm = Main.permutation(primeNumbers);
					System.out.println(perm);
					// for (int in = RECORDS - 1; in >= 0; --in) {
					for (Integer in : perm) {
						System.out.println("--------------DELE " + in);
						delete(in, DBLock.E);
						print(DBLock.E);
						// Thread.sleep(100);
					}
					print(DBLock.E); // must be -1::0
					// Thread.sleep(2000);
					commit();
					// eng.print(tr, DBLock.E);
				}
			};
			eng.submit(to);
			// ArrayList<Inserter<T>> arrayList = new ArrayList<>();
			// ArrayList<Lookuper<T>> arrayList = new ArrayList<>();
			// ArrayList<InserterDeleter<T>> arrayList = new ArrayList<>();
			// for (int i = 0; i < 100; ++i)
			// arrayList.add(new InserterDeleter<T>(eng, new Record<>(0, 0)));
			// arrayList.add(new Lookuper<T>(eng, i));
			// arrayList.add(new Inserter<T>(eng, new Record<>(i, i)));
			// List<Future<T>> invokeAll = exec.invokeAll(arrayList, 1000000,
			// TimeUnit.MILLISECONDS);
			// for (Future<T> future : invokeAll) {
			// try {
			// future.get();
			// } catch (ExecutionException e) {
			// // TODO policy
			// e.printStackTrace();
			// }
			// }
		} finally {
			eng.shutdown();
		}
	}

	@SuppressWarnings("unused")
	private static void treePrint(Engine<Integer, Integer, Integer> eng)
			throws FileNotFoundException, IOException, InterruptedException,
			TransactionRequiredException, KeyExistsException,
			TransactionFailedException {
		List<Integer> primeNumbers = new ArrayList<>(Arrays.asList(2, 3, 5, 7,
			11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 61, 59, 67, 71, 73,
			79, 83, 89, 97, 101, 103, 109, 113, 127, 131, 137, 139, 149, 151,
			157, 163, 167, 173, 179, 107));
		// for (int j = 0; ++j < 10;) {
		List<Integer> perm = permutation(primeNumbers);
		System.out.println(perm);
		Transaction tr = eng.beginTransaction();
		for (Integer in : perm) {
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
}
