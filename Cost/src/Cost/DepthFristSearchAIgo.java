package Cost;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Stack;

public class DepthFristSearchAIgo implements ISearchAlgo{

	@Override
	public void execute(Node tree) {
		// TODO Auto-generated method stub
		Stack<Node> frontier = new Stack<Node>();
		List<Node> explored = new ArrayList<Node>();
		frontier.add(tree);
		while (!frontier.isEmpty()) {
			Node node = frontier.pop();
			explored.add(node);
			System.out.println(node.getLabel());

			ArrayList<Node> children = (ArrayList<Node>) node.getChildrenNodes();
			Collections.sort(children);
			for (int i = 0; i < children.size(); i++) {
				Node n = children.get(i);
				if (!frontier.contains(n) && !explored.contains(n)) {
					frontier.add(n);
				}
			}
		}
		System.out.println();
	}

	@Override
	public Node execute(Node tree, String goal) {
		// TODO Auto-generated method stub
		Stack<Node> frontier = new Stack<Node>();
		ArrayList<Node> explored = new ArrayList<Node>();
		frontier.add(tree);
		while (!frontier.isEmpty()) {

			Node node = frontier.pop();
			explored.add(node);
			System.out.println(node.getLabel());
			if (node.getLabel().equals(goal)) {
				System.out.println(node.getPathCost());
				return node;
			}

			List<Edge> childEdge = node.getChildren();
			for (int a = 0; a < childEdge.size(); a++) {
				Edge edge = childEdge.get(a);
				Node no = edge.getEnd();
				if (!(explored.contains(no)) && !(frontier.contains(no))) {
					double cost = node.getPathCost() + edge.getWeight();
					no.setPathCost(cost);
					no.setParent(node);
					frontier.add(no);
				} else if (frontier.contains(no) && (no.getPathCost() > (node.getPathCost()))) {
					double cost = node.getPathCost() + edge.getWeight();
					no.setPathCost(cost);
					no.setParent(no);
				}
			}
			List<Node> children = node.getChildrenNodes();
			for (int i = 0; i < children.size(); i++) {
				Node n = children.get(i);
				if (!(frontier.contains(n)) && !(explored.contains(n))) {
					frontier.add(n);
					n.setParent(node);
				}
			}

			System.out.println("Frontier: " + frontier);
		}
		return null;
	}

	@Override
	public Node execute(Node tree, String start, String goal) {
		// TODO Auto-generated method stub
		Stack<Node> frontier = new Stack<Node>();
		ArrayList<Node> explored = new ArrayList<Node>();
		Stack<Node> traiter = new Stack<Node>();
		boolean started = false;
		frontier.add(tree);
		while (!(frontier.isEmpty())) {

			Node node = frontier.pop();
			explored.add(node);
			System.out.println(node.getLabel());
			if (node.getLabel().equals(start)) {
				started = true;
				frontier.clear();
				explored.clear();
			} else if (node.getLabel().equals(goal)) {
				System.out.println(node.getPathCost());
				return node;
			}

			List<Edge> childEdge = node.getChildren();
			for (int a = 0; a < childEdge.size(); a++) {
				Edge edge = childEdge.get(a);
				Node no = edge.getEnd();
				if (!(explored.contains(no)) && !(frontier.contains(no))) {
					double cost = node.getPathCost() + edge.getWeight();
					no.setPathCost(cost);
					no.setParent(node);
					frontier.add(no);
				} else if (frontier.contains(no) && (no.getPathCost() > (node.getPathCost()))) {
					double cost = node.getPathCost() + edge.getWeight();
					no.setPathCost(cost);
					no.setParent(no);
				}
			}

			List<Node> children = node.getChildrenNodes();
			for (int i = 0; i < children.size(); i++) {
				Node n = children.get(i);
				if (!(frontier.contains(n)) && !(explored.contains(n))) {
					frontier.add(n);
					n.setParent(node);
				}
			}

			System.out.println("Frontier: " + frontier);
		}
		return null;
	}


}
