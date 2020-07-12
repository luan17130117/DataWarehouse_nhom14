package InformedSearch;

import java.util.ArrayList;

import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class AStarSearchAlgo implements IInformedSearchAlgo {

	@Override
	public Node execute(Node tree, String goal) {
		PriorityQueue<Node> frontier = new PriorityQueue<Node>(new NodeComparatorF());
		ArrayList<Node> explored = new ArrayList<>();
		tree.setG(0);
		frontier.add(tree);
		while (!frontier.isEmpty()) {
			System.out.println(frontier);
			Node currentNode = frontier.poll();
			System.out.print(currentNode.getLabel() + "\t");
			if (currentNode.getLabel().equals(goal)) {
				return currentNode;
			}
			explored.add(currentNode);
			List<Edge> children = currentNode.getChildren();
			for (Edge e : children) {
				Node child = e.getEnd();
				double oldCostF = child.getF();
				double newCostF = e.getWeight() + currentNode.getG() + child.getH();
				if (!explored.contains(child) && !frontier.contains(child)) {
					child.setParent(currentNode);
					child.setG(e.getWeight() + currentNode.getG());
					frontier.add(child);
				} else if (frontier.contains(child) && newCostF < oldCostF) {
					child.setG(e.getWeight() + currentNode.getG());
					child.setParent(currentNode);
					frontier.add(child);

				}
			}
		}

		return null;

	}

	class NodeComparatorF implements Comparator<Node> {
		@Override
		public int compare(Node o1, Node o2) {
			Double f1 = o1.getF();
			Double f2 = o2.getF();
			int result = f1.compareTo(f2);
			if (result == 0)
				return o1.getLabel().compareTo(o2.getLabel());
			else
				return result;
		}
	}

	@Override
	public Node execute(Node tree, String start, String goal) {
		boolean foundStart = false;
		PriorityQueue<Node> frontier = new PriorityQueue<Node>(new NodeComparatorF());
		ArrayList<Node> explored = new ArrayList<Node>();
		tree.setG(0);
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
				double oldCostF = node.getF();
				double newCostF = e.getWeight() + first.getG() + node.getH();
				if (!frontier.contains(node) && !explored.contains(node)) {
					if (foundStart) {
						node.setG(first.getG() + e.getWeight());
						node.setParent(first);
					}
					frontier.add(node);
				} else if (frontier.contains(node) && newCostF < oldCostF) {
					if (foundStart) {
						node.setG(first.getG() + e.getWeight());
						node.setParent(first);
					}
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
		
//		new AStarSearchAlgo().execute(s, g.getLabel());
		IInformedSearchAlgo aStar = new AStarSearchAlgo();
//		Node res1 = aStar.execute(s, g.getLabel());
//		System.out.println(NodeUtils.printPath(res1));
		Node res2 = aStar.execute(s, s.getLabel(), g.getLabel());
		System.out.println(NodeUtils.printPath(res2));
		
	}

}
