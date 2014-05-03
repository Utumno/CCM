package gr.uoa.di.mde515;

import gr.uoa.di.mde515.engine.CCM.TransactionRequiredException;
import gr.uoa.di.mde515.engine.Engine;
import gr.uoa.di.mde515.engine.Engine.TransactionFailedException;
import gr.uoa.di.mde515.engine.Transaction;
import gr.uoa.di.mde515.index.Index.KeyExistsException;
import gr.uoa.di.mde515.index.Record;
import gr.uoa.di.mde515.trees.BPlusJava;
import gr.uoa.di.mde515.trees.BPlusTree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Main {

	static Random r = new Random();

	public static void main(String[] args) throws TransactionRequiredException,
			KeyExistsException, TransactionFailedException,
			InterruptedException, IOException {
		Engine<Integer, Integer> eng = Engine.newInstance();
		try {
			Transaction tr = eng.beginTransaction();
			for (int i = 0; i < 100; i++) {
				Record<Integer, Integer> rec = new Record<>(i, i);
				eng.insert(tr, rec);
			}
			// eng.e_xaction(tr);
			eng.print();
		} finally {
			eng.shutEngine();
		}
		// treePrint();
	}

	private static void treePrint() {
		List<Integer> primeNumbers = new ArrayList<>(Arrays.asList(2, 3, 5, 7,
			11, 13, 19, 23, 37, 41, 43, 47, 53, 59, 67, 71, 61, 73, 79, 89, 97,
			101, 103, 109, 29, 31, 113, 127, 131, 137, 139, 149, 151, 157, 163,
			167, 173, 179, 17, 83, 107));
		for (int j = 0; ++j < 10;) {
			List<Integer> perm = permutation(primeNumbers);
			// List<Integer> perm = primeNumbers;
			BPlusTree<Integer, Integer> bPlusTree = new BPlusJava<>();
			System.out.println(perm);
			for (int i = 0; i < perm.size(); i++) {
				final Integer in = perm.get(i);
				Record<Integer, Integer> rec = new Record<>(in, in);
				bPlusTree.insert(rec);
				// Thread.sleep(1000);
				// bPlusTree.print();
			}
			bPlusTree.print();
			System.out.println();
		}
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
