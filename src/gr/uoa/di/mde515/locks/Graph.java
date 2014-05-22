package gr.uoa.di.mde515.locks;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.CycleDetector;
import org.jgrapht.alg.cycle.JohnsonSimpleCycles;
import org.jgrapht.graph.DefaultEdge;

public class Graph {

	public void breakCycle(DirectedGraph<String, DefaultEdge> graph) {
		// local to the method
		JohnsonSimpleCycles<String, DefaultEdge> j = new JohnsonSimpleCycles<>(
			graph);// using the graph
		// using the graph+ local to the method
		CycleDetector<String, DefaultEdge> t = new CycleDetector<>(graph);
		while (t.detectCycles()) {
			// local to the method
			List<List<String>> findS = j.findSimpleCycles();
			System.out.println("The cycle(s) are " + findS);
			// local to the method
			Map<String, Integer> map = new HashMap<>();
			for (int i = 0; i < findS.size(); i++) {
				for (int j1 = 0; j1 < findS.get(i).size(); j1++) {
					String key = findS.get(i).get(j1);
					if (map.containsKey(key)) {
						int value = map.get(key);
						map.put(key, value + 1);
					} else {
						map.put(key, 1);
					}
				}
			}
			Entry<String, Integer> entry = findMax(map);
			String str = entry.getKey();
			int freq = entry.getValue();
			if (freq > 1) {
				System.out.println("The vertex to be removed is " + str);
				graph.removeVertex(str);// using the graph structure
										// needs synchronization
				System.out.println("After removal");
			} else {
				System.out.println("The vertex to be removed is " + str);
				graph.removeVertex(str);// using the graph structure
										// needs synchronization
				System.out.println("After removal");
			}
			System.out.println("Does it contain a cycle? " + t.detectCycles());
			List<List<String>> findSA = j.findSimpleCycles();
		}
	}

	private Entry<String, Integer> findMax(Map<String, Integer> map) {
		Map.Entry<String, Integer> maxEntry = null;
		for (Map.Entry<String, Integer> entry : map.entrySet()) {
			if (maxEntry == null
				|| entry.getValue().compareTo(maxEntry.getValue()) > 0) {
				maxEntry = entry;
			}
		}
		return maxEntry;
	}
}
