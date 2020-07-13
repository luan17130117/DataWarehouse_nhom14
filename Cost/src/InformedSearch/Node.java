package InformedSearch;

import java.util.ArrayList;
import java.util.List;

public class Node {
	private String label;
	private Node parent; // for print the path from the start node to goal node
	private double g;// from Start to this node
	private double h;// from this node to the goal
	private List<Edge> children = new ArrayList<Edge>();
	private double pathCost;

	public double getPathCost() {
		return pathCost;
	}

	public void setPathCost(double pathCost) {
		this.pathCost = pathCost;
	}

	public Node(String label) {
		this.label = label;
	}

	protected Node(String label, double h) {
		super();
		this.label = label;
		this.h = h;
	}

	public double getF() {
		return this.g + this.h;
	}

	public double getG() {
		return g;
	}

	public void setG(double g) {
		this.g = g;
	}

	public String getLabel() {
		return label;
	}

	public Node getParent() {
		return parent;
	}

	public void setParent(Node parent) {
		this.parent = parent;
	}

	public void setChildren(List<Edge> children) {
		this.children = children;
	}

	public List<Edge> getChildren() {
		return children;
	}

	public List<Node> getChildrenNodes() {
		List<Node> result = new ArrayList<Node>();
		for (Edge edge : this.children) {
			result.add(edge.getEnd());
		}
		return result;
	}

	// an edge is generated using this node and "that" with the given cost
	public void addEdge(Node that, double cost) {
		Edge edge = new Edge(this, that, cost);
		this.children.add(edge);
	}

	// an edge is generated using this node and "that" with the given cost
	public void addEdge(Node that) {
		Edge edge = new Edge(this, that);
		this.children.add(edge);
	}

	public double getH() {
		return h;
	}

	public void setH(double h) {
		this.h = h;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((label == null) ? 0 : label.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Node other = (Node) obj;
		if (label == null) {
			if (other.label != null)
				return false;
		} else if (!label.equals(other.label))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return this.label + " " + this.getF();
	}
}