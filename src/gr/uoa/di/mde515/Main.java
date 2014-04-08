package gr.uoa.di.mde515;

import gr.uoa.di.mde515.db.BPlus;
import gr.uoa.di.mde515.db.Record;

public class Main {

	public static void main(String[] args) throws InterruptedException {
		// Engine eng = Engine.newInstance(); // FIXME SINGLETON
		// Transaction tr = eng.b_xaction();
		// Record<Integer> rec = null;
		// eng.insert(tr, rec);
		// eng.e_xaction(tr);
		BPlus<Integer, Integer> bPlusTree = new BPlus<Integer, Integer>();
		int primeNumbers[] = new int[] { 2, 3, 5, 7, 11, 13, 19, 23, 37, 41,
				43, 47, 53, 59, 67, 71, 61, 73, 79, 89, 97, 101, 103, 109, 29,
				31, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179,
				17, 83, 107 };
		for (int i = 0; i < primeNumbers.length; i++) {
			Record<Integer, Integer> rec = new Record<>(primeNumbers[i],
				primeNumbers[i]);
			bPlusTree.insert(rec);
			// Thread.sleep(1000);
		}
		bPlusTree.print();
	}
}
