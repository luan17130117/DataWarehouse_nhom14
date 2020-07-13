package UniformCost;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class Uniformcost implements ISearchAlgo {

	@Override
	public void execute(Node tree) {
		PriorityQueue<Node> frontier = new PriorityQueue<Node>(1, new Comparator<Node>() {

			public int compare(Node o1, Node o2) {

				return Double.compare(o1.getPathCost(), o2.getPathCost());
			}
		});
		ArrayList<Node> explored = new ArrayList<Node>();
		frontier.add(tree);
		while (!frontier.isEmpty()) {
			Node first = frontier.poll();
			System.out.println(first.getLabel() + "\t");
			explored.add(first);
			List<Edge> edge = first.getChildren();
			for (Edge e : edge) {
				Node node = e.getEnd();
				node.setParent(first);
				if (!frontier.contains(node)) {
					node.setPathCost(first.getPathCost() + e.getWeight());
					frontier.add(node);
				} else if (frontier.contains(node) && node.getPathCost() > first.getPathCost() + e.getWeight()) {
					node.setPathCost(first.getPathCost() + e.getWeight());
				}
			}
		}
	}

	public Node execute(Node tree, String goal) {
		PriorityQueue<Node> frontier = new PriorityQueue<Node>(1, new Comparator<Node>() {

			public int compare(Node o1, Node o2) {

				return Double.compare(o1.getPathCost(), o2.getPathCost());
			}

		});
		ArrayList<Node> explored = new ArrayList<Node>();
		frontier.add(tree);
		while (!frontier.isEmpty()) {
			System.out.println(frontier);
			Node first = frontier.poll();
			if (first.getLabel().equals(goal)) {
				return first;
			}
			explored.add(first);
			List<Edge> edge = first.getChildren();
			for (Edge e : edge) {
				Node node = e.getEnd();
				if (!frontier.contains(node) && !explored.contains(node)) {
					node.setPathCost(first.getPathCost() + e.getWeight());
					node.setParent(first);
					frontier.add(node);
				} else if (frontier.contains(node) && node.getPathCost() > first.getPathCost() + e.getWeight()) {
					node.setParent(first);
					node.setPathCost(first.getPathCost() + e.getWeight());
				}

			}
		}
		return null;
	}

	public Node execute(Node tree, String start, String goal) {
		boolean foundStart = false;
		PriorityQueue<Node> frontier = new PriorityQueue<Node>(1, new Comparator<Node>() {

			public int compare(Node o1, Node o2) {

				return Double.compare(o1.getPathCost(), o2.getPathCost());
			}

		});
		ArrayList<Node> explored = new ArrayList<Node>();
		frontier.add(tree);
		while (!frontier.isEmpty()) {
			System.out.println(frontier);
			Node first = frontier.poll();
			System.out.println(first.getLabel() + "\t");
			if (first.getLabel().equals(start)) {
				frontier.removeAll(frontier);
				foundStart = true;
			}
			if (foundStart) {
				explored.add(first);
			}
			if (first.getLabel().equals(goal) && foundStart) {
				return first;
			}
			List<Edge> edge = first.getChildren();
			for (Edge e : edge) {
				Node node = e.getEnd();
				if (!frontier.contains(node) && !explored.contains(node)) {
					if (foundStart) {
						node.setPathCost(first.getPathCost() + e.getWeight());
						node.setParent(first);
					}
					frontier.add(node);
				} else if (frontier.contains(node) && node.getPathCost() > first.getPathCost() + e.getWeight()) {
					if (foundStart) {
						node.setPathCost(first.getPathCost() + e.getWeight());
						node.setParent(first);
					}
				}

			}
		}
		return null;
	}

	public static void main(String[] args) {
		Node s = new Node("S");
		Node a = new Node("A");
		Node c = new Node("C");
		Node e = new Node("E");
		Node g = new Node("G");
		Node b = new Node("B");
		Node d = new Node("D");
		Node f = new Node("F");
		Node h = new Node("H");

		s.addEdge(b, 3);
		s.addEdge(a, 2);
		a.addEdge(c, 3);
		b.addEdge(d, 3);
		b.addEdge(c, 1);
		c.addEdge(e, 3);
		c.addEdge(d, 1);
		d.addEdge(f, 2);
		f.addEdge(g, 1);
		e.addEdge(g, 2);
		new Uniformcost().execute(s, g.getLabel());
	}
}
