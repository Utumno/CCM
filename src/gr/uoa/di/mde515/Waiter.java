package gr.uoa.di.mde515;

import gr.uoa.di.mde515.engine.Engine;
import gr.uoa.di.mde515.engine.Transaction;

import java.util.concurrent.Callable;

final class Waiter<T> implements Callable<T> {

	private final Engine<Integer, Integer, Integer> eng;

	Waiter(Engine<Integer, Integer, Integer> eng) {
		this.eng = eng;
	}

	@Override
	public T call() throws Exception {
		Transaction tr = eng.beginTransaction();
		try {
			System.out.println("Before wait ");
			eng.waitTransaction(10000);
			System.out.println("After wait ");
		} finally {
			eng.endTransaction(tr);
		}
		return null;
	}
}