package gr.uoa.di.mde515.locks;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.CycleDetector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;

public class GraphMain {

	public static void main(String[] args) {
		DirectedGraph<String, DefaultEdge> graph = new SimpleDirectedGraph<>(
			DefaultEdge.class);
		Graph g = new Graph();
		String G = "G";
		String L = "L";
		String K = "K";
		String D = "D";
		String M = "M";
		// add the vertices
		graph.addVertex(G);
		graph.addVertex(L);
		graph.addVertex(K);
		graph.addVertex(D);
		graph.addVertex(M);
		// add edges to create a circuit
		graph.addEdge(G, L);
		graph.addEdge(L, G);
		graph.addEdge(K, L);
		graph.addEdge(L, K);
		graph.addEdge(D, M);
		graph.addEdge(M, D);
		System.out.println(graph.toString());
		System.out.println("Does it contain the edge? "
			+ graph.containsEdge(G, D));
		CycleDetector<String, DefaultEdge> t = new CycleDetector<>(graph);
		System.out.println("Does it contain a cycle? " + t.detectCycles());
		System.out.println("Is the M part of the cycle? "
			+ t.detectCyclesContainingVertex(M));
		Set<String> s = new TreeSet<>();
		System.out.println("The find cycles " + t.findCycles());
		System.out.println("The cycle containing vertex M is "
			+ t.findCyclesContainingVertex(M));
		s = t.findCyclesContainingVertex(D);
		Iterator<String> iter = s.iterator();
		while (iter.hasNext()) {
			System.out.println(iter.next() + " ");
		}
		g.breakCycle(graph);
	}
}
