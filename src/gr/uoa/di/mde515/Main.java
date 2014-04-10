package gr.uoa.di.mde515;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Main {

	static Random r = new Random();

	// public static void main(String[] args) {
	// // Engine eng = Engine.newInstance(); // FIXME SINGLETON
	// // Transaction tr = eng.b_xaction();
	// // Record<Integer> rec = null;
	// // eng.insert(tr, rec);
	// // eng.e_xaction(tr);
	// List<Integer> primeNumbers = new ArrayList<>(Arrays.asList(2, 3, 5, 7,
	// 11, 13, 19, 23, 37, 41, 43, 47, 53, 59, 67, 71, 61, 73, 79, 89, 97,
	// 101, 103, 109, 29, 31, 113, 127, 131, 137, 139, 149, 151, 157, 163,
	// 167, 173, 179, 17, 83, 107));
	// for (int j = 0; ++j < 10;) {
	// List<Integer> perm = permutation(primeNumbers);
	// // List<Integer> perm = primeNumbers;
	// BPlusTree<Integer, Integer> bPlusTree = new BPlusJava<>();
	// System.out.println(perm);
	// for (int i = 0; i < perm.size(); i++) {
	// final Integer in = perm.get(i);
	// Record<Integer, Integer> rec = new Record<>(in, in);
	// bPlusTree.insert(rec);
	// // Thread.sleep(1000);
	// // bPlusTree.print();
	// }
	// bPlusTree.print();
	// System.out.println();
	// }
	// }
	public static void main(String[] args) throws InterruptedException,
			IOException {
		int sum = 0;
		int[] warmup = new int[1];
		warmup[0] = 1;
		for (int i = 0; i < 15000; i++) { // triggers JIT
			sum += copyClone(warmup);
			sum += copyArrayCopy(warmup);
			sum += copyCopyOf(warmup);
		}

		int count = 10_000_000;
		int[] array = new int[count];
		for (int i = 0; i < count; i++) {
			array[i] = i;
		}

		// additional warmup for main
		for (int i = 0; i < 10; i++) {
			sum += copyArrayCopy(array);
		}
		System.gc();
		// copyClone
		long start = System.nanoTime();
		for (int i = 0; i < 10; i++) {
			sum += copyClone(array);
		}
		long end = System.nanoTime();
		System.out.println("clone: " + (end - start) / 1000000);
		System.gc();
		// copyArrayCopy
		start = System.nanoTime();
		for (int i = 0; i < 10; i++) {
			sum += copyArrayCopy(array);
		}
		end = System.nanoTime();
		System.out.println("arrayCopy: " + (end - start) / 1000000);
		System.gc();
		// copyCopyOf
		start = System.nanoTime();
		for (int i = 0; i < 10; i++) {
			sum += copyCopyOf(array);
		}
		end = System.nanoTime();
		System.out.println("Arrays.copyOf: " + (end - start) / 1000000);
		// sum
		System.out.println(sum);
	}

	private static int copyClone(int[] array) {
		int[] copy = array.clone();
		return copy[copy.length - 1];
	}

	private static int copyArrayCopy(int[] array) {
		int[] copy = new int[array.length];
		System.arraycopy(array, 0, copy, 0, array.length);
		return copy[copy.length - 1];
	}

	private static int copyCopyOf(int[] array) {
		int[] copy = Arrays.copyOf(array, array.length);
		return copy[copy.length - 1];
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
