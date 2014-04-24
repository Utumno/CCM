package gr.uoa.di.mde515.engine.buffer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class FramePage {

	// (key)pageid me framenumber(value)
	Map<Integer, Integer> map = new HashMap<>();

	public void setKeyValue(int key, int value) {
		map.put(key, value);
	}

	public void removeKey() {}

	public boolean hasKey(int key) {
		return map.containsKey(key);
	}

	public int hasValue(int key) {
		return map.get(key);
	}

	public void iter() {
		for (Iterator<Integer> iterator = map.keySet().iterator(); iterator
			.hasNext();) {
			Integer key = iterator.next();
			System.out.println("The hashmap values are " + map.get(key));
		}
	}
}
