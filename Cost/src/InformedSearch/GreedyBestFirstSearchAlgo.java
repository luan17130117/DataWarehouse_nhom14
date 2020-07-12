package InformedSearch;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class GreedyBestFirstSearchAlgo implements IInformedSearchAlgo {

	@Override
	public Node execute(Node tree, String goal) {
		List<String> path = new ArrayList<String>();
		PriorityQueue<Node> frontier = new PriorityQueue<Node>(new Comparator<Node>() {

			@Override
			public int compare(Node n1, Node n2) {
				Double h1 = n1.getH();
				Double h2 = n2.getH();
				int result = h1.compareTo(h2);
				if (result == 0) {
					result = n1.getLabel().compareTo(n2.getLabel());
				}
				return result;
			}
		});
		frontier.add(tree);
		List<Node> explored = new ArrayList<Node>();
		while (!frontier.isEmpty()) {
			Node currentNode = frontier.remove();
			if (null != currentNode.getParent()) {
				if (!path.contains(currentNode.getParent().getLabel())) {
					path.add(currentNode.getParent().getLabel());
				}
			}
			if (currentNode.getLabel().equals(goal)) {
				path.add(goal);
				for (String element : path)
					System.out.print(element + "\t");
//				System.out.println(sumPath(tree, goal));
				return currentNode;
			}
			List<Edge> edges = currentNode.getChildren();
			for (Edge edge : edges) {
				Node node = edge.getEnd();
				if (!frontier.contains(node) && !explored.contains(node)) {
					frontier.add(node);
					node.setParent(currentNode);
				}
			}

//			for (Node node : children) {
//				if (!frontier.contains(node) && !explored.contains(node)) {
//					frontier.add(node);
//					node.setParent(currentNode);
//				}
//			}

		}

		return null;

	}

	public double sumPath(Node tree, String goal) {
		double result = 0.0;

		if (!tree.getLabel().equalsIgnoreCase(goal)) {
			result += (new Edge(tree, tree.getParent()).getWeight());
//			sumPath(tree.getParent(), goal);
		}

		return result;

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
		new GreedyBestFirstSearchAlgo().execute(s, g.getLabel());

	}

	@Override
	public Node execute(Node tree, String start, String goal) {
		return null;
	}


}
