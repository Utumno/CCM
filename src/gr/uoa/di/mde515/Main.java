package gr.uoa.di.mde515;

import gr.uoa.di.mde515.db.Record;
import gr.uoa.di.mde515.engine.Engine;
import gr.uoa.di.mde515.engine.Transaction;

public class Main {

	public static void main(String[] args) {
		Engine eng = Engine.newInstance(); // FIXME SINGLETON
		Transaction tr = eng.b_xaction();
		Record<Integer> rec = null;
		eng.insert(tr, rec);
		eng.e_xaction(tr);
	}
}
