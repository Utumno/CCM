package gr.uoa.di.mde515.engine.buffer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

final class FramePage {

	// (key)pageid me framenumber(value)
	Map<Integer, Integer> map = new HashMap<>();

	public void setKeyValue(int key, int value) {
		map.put(key, value);
	}

	public void removeKey(int key) {
		map.remove(key);
	}

	public boolean hasKey(int key) {
		return map.containsKey(key);
	}

	public boolean hasValue(int value) {
		return map.containsValue(value);
	}

	public int getValue(int key) {
		return map.get(key);
	}

	public void getKey() {
		for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
			System.out.println("(map)The key value is " + entry.getKey());
		}
	}

	public void iter() {
		for (Iterator<Integer> iterator = map.keySet().iterator(); iterator
			.hasNext();) {
			Integer key = iterator.next();
			System.out.println("The hashmap values are " + map.get(key));
		}
	}
}
