package Cost;

public class TestDFS {
	public static void main(String[] args) {
		Node nodeS = new Node("S");
		Node nodeA = new Node("A");
		Node nodeC = new Node("C");
		Node nodeE = new Node("E");
		Node nodeG = new Node("G");
		Node nodeB = new Node("B");
		Node nodeD = new Node("D");
		Node nodeF = new Node("F");
		Node nodeH = new Node("H");
		nodeS.addEdge(nodeA, 5);
		nodeS.addEdge(nodeC, 4);
		nodeS.addEdge(nodeB, 2);
		nodeA.addEdge(nodeE, 4);
		nodeA.addEdge(nodeD, 9);
		nodeB.addEdge(nodeG, 6);
		nodeC.addEdge(nodeF, 2);
		nodeD.addEdge(nodeH, 7);
		nodeE.addEdge(nodeG, 6);
		nodeF.addEdge(nodeG, 1);
		ISearchAlgo algol = new DepthFristSearchAIgo();
		algol.execute(nodeS);
		System.out.println();
		algol.execute(nodeS, "H");
		System.out.println();
		algol.execute(nodeS, "A", "H");

	}
}
