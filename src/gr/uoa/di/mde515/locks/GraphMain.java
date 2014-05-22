package gr.uoa.di.mde515.locks;

import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;

import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.CycleDetector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;

public class GraphMain {

	public static void main(String[] args) {
		DirectedGraph<String, DefaultEdge> graph = new SimpleDirectedGraph<String, DefaultEdge>(
			DefaultEdge.class);
		Graph g = new Graph();
		Scanner input = new Scanner(System.in);
		System.out.println("Give the numbers of vertices: ");
		int numOfVertices = input.nextInt();
		System.out.println("Give name of vertices: ");
		for (int i = 0; i < numOfVertices; i++) {
			graph.addVertex(input.next());
		}
		System.out.println("Enter the number of edges: ");
		int numOfEdges = input.nextInt();
		for (int j = 0; j < numOfEdges; j++) {
			System.out.println("Start point: ");
			String start = input.next();
			System.out.println("End point: ");
			String end = input.next();
			graph.addEdge(start, end);
		}
		System.out.println("The graph is ");
		System.out.println(graph.toString());
		CycleDetector<String, DefaultEdge> t = new CycleDetector<>(graph);
		System.out.println("Does it contain a cycle? " + t.detectCycles());
		Set<String> s = new TreeSet<String>();
		Iterator<String> iter = s.iterator();
		while (iter.hasNext()) {
			System.out.println(iter.next() + " ");
		}
		g.breakCycle(graph);
	}
}
