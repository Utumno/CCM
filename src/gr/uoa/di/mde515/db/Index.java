package gr.uoa.di.mde515.db;


public class Index<T> {

	BPlusTree Bpluuuuuuusususuususu;

	public Record<T> find(T key) {
		return (Record<T>) Bpluuuuuuusususuususu.search((Integer) key);
	}
}
