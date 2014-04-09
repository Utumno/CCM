package gr.uoa.di.mde515;

import gr.uoa.di.mde515.db.BPlus;
import gr.uoa.di.mde515.db.Record;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Main {

	static Random r = new Random();

	public static void main(String[] args) {
		// Engine eng = Engine.newInstance(); // FIXME SINGLETON
		// Transaction tr = eng.b_xaction();
		// Record<Integer> rec = null;
		// eng.insert(tr, rec);
		// eng.e_xaction(tr);
		List<Integer> primeNumbers = new ArrayList<>(Arrays.asList(2, 3, 5, 7,
			11, 13, 19, 23, 37, 41, 43, 47, 53, 59, 67, 71, 61, 73, 79, 89, 97,
			101, 103, 109, 29, 31, 113, 127, 131, 137, 139, 149, 151, 157, 163,
			167, 173, 179, 17, 83, 107));
		for (int j = 0; ++j < 2;) {
			List<Integer> perm = permutation(primeNumbers);
			// List<Integer> perm = primeNumbers;
			BPlus<Integer, Integer> bPlusTree = new BPlus<>();
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
		primeNumbers = new ArrayList<>(primeNumbers);
		final List<Integer> perm = new ArrayList<>();
		for (int inLen = primeNumbers.size(); inLen != 0; --inLen) {
			int nextInt = r.nextInt(inLen);
			perm.add(primeNumbers.remove(nextInt));
		}
		return perm;
	}
}
