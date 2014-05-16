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

public class Main {

	private static Random r = new Random();

	public static void main(String[] args) throws TransactionRequiredException,
			KeyExistsException, TransactionFailedException,
			InterruptedException, IOException {
		Engine<Integer, Integer> eng = Engine.newInstance();
		try {
			treePrint(eng);
			// Transaction tr = eng.beginTransaction();
			// for (int i = 0; i < 100; i++) {
			// Record<Integer, Integer> rec = new Record<>(i, i);
			// eng.insert(tr, rec);
			// }
			// eng.commit(tr);
			// // eng.e_xaction(tr);
			// eng.print();
		} finally {
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
}
