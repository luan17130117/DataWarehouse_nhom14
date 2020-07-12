package Cost;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class BreadthFristSearchAIgo implements ISearchAlgo {

	@Override
	public void execute(Node tree) {
		Queue<Node> frontier = new LinkedList<Node>();
		frontier.add(tree);
		List<Node> expored = new ArrayList<Node>();

		while (!frontier.isEmpty()) {
			Node currentNode = frontier.remove();
			expored.add(currentNode);
			System.out.println(currentNode.getLabel() + " ");

			List<Node> childrent = currentNode.getChildrenNodes();
			Collections.sort(childrent);
			for (Node n : childrent) {
				if (!(frontier.contains(n)) && !(expored.contains(n))) {
					frontier.add(n);
				}
			}
		}
	}

	@Override
	public Node execute(Node tree, String goal) {
		Queue<Node> frontier = new LinkedList<Node>();
		frontier.add(tree);
		List<Node> expored = new ArrayList<Node>();

		while (!frontier.isEmpty()) {
			Node currentNode = frontier.remove();
			expored.add(currentNode);
			System.out.println(currentNode.getLabel() + " ");
			if (currentNode.getLabel().equals(goal)) {
				return currentNode;
			}

			List<Node> childrent = currentNode.getChildrenNodes();
			for (int i = 0; i < childrent.size(); i++) {
				Node n = childrent.get(i);
				if (!frontier.contains(n) && !expored.contains(n)) {
					frontier.add(n);
					n.setParent(currentNode);
				}
			}
		}
		System.out.println();
		return null;
	}

	@Override
	public Node execute(Node tree, String start, String goal) {
		Queue<Node> frontier = new LinkedList<Node>();
		frontier.add(tree);
		List<Node> expored = new ArrayList<Node>();
		boolean started = false;
		while (!frontier.isEmpty()) {
			Node currentNode = frontier.remove();
			expored.add(currentNode);
			System.out.println(currentNode.getLabel() + " ");

			if (currentNode.getLabel().equals(start)) {
				started = true;
//				frontier.clear();
//				expored.clear();
			}
			
			if (currentNode.getLabel().equals(goal) && started) {
				return currentNode;
			}

			List<Edge> childrent = currentNode.getChildren();
			for (int i = 0; i < childrent.size(); i++) {
				Edge tmp = childrent.get(i);
				Node n = tmp.getEnd();

				if (!frontier.contains(n) && !expored.contains(n)) {
					double cost = currentNode.getPathCost() + tmp.getWeight();
					n.setPathCost(cost);
					n.setParent(currentNode);
					frontier.add(n);
				} else if (frontier.contains(n) && (n.getPathCost() > (currentNode.getPathCost() + tmp.getWeight()))) {
					double cost = currentNode.getPathCost() + tmp.getWeight();
					n.setParent(currentNode);
					n.setPathCost(cost);

				}
			}
			System.out.println("Frontier: " + frontier);
		}
		
		return null;
		
	}

}
