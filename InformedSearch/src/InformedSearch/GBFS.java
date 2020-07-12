package InformedSearch;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class GBFS implements IInformedSearchAlgo {

	@Override
	public InformedSearch.Node execute(InformedSearch.Node tree, String goal) {
		// TODO Auto-generated method stub
		PriorityQueue<Node> frontier = new PriorityQueue<Node>(new NodeComparatorByGn());
		ArrayList<Node> explored = new ArrayList<Node>();
		frontier.add(tree);
		while (!frontier.isEmpty()) {
			System.out.println(frontier);
			Node first = frontier.poll();
			System.out.print(first.getLabel() + " ");
			if (first.getLabel().equals(goal)) {
				return first;
			}
			explored.add(first);
			List<Edge> edge = first.getChildren();
			for (Edge e : edge) {
				Node node = e.getEnd();
				if (!frontier.contains(node) && !explored.contains(node)) {
					node.setG(first.getG());
					node.setParent(first);
					frontier.add(node);
				}

			}
		}
		return null;
	}

	@Override
	public Node execute(Node tree, String start, String goal) {
		boolean foundStart = false;
		PriorityQueue<Node> frontier = new PriorityQueue<Node>(new NodeComparatorByGn());
		ArrayList<Node> explored = new ArrayList<Node>();
		frontier.add(tree);
		while (!frontier.isEmpty()) {
			System.out.println(frontier);
			Node first = frontier.poll();
			System.out.print(first.getLabel() + "\t");
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
						node.setG(first.getG());
						node.setParent(first);
					}
					frontier.add(node);

				}

			}
		}
		return null;
	}

	public static void main(String[] args) {
		Node s = new Node("S", 6);
		Node b = new Node("B", 4);
		Node a = new Node("A", 4);
		Node c = new Node("C", 4);
		Node d = new Node("D", 3.5);
		Node e = new Node("E", 1);
		Node f = new Node("F", 1);
		Node g = new Node("G", 0);

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
//		new GBFS().execute(s, g.getLabel());
		IInformedSearchAlgo aStar = new GBFS();
//		Node res1 = aStar.execute(s, g.getLabel());
//		System.out.println(NodeUtils.printPath(res1));
		Node res2 = aStar.execute(s, s.getLabel(), g.getLabel());
		System.out.println(NodeUtils.printPath(res2));
	}

}
