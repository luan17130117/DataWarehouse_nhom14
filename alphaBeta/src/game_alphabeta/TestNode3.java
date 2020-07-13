package game_alphabeta;

public class TestNode3 {
	public static void main(String[] args) {
		Node root = new Node("A");
		Node nodeB = new Node("B");
		Node nodeC = new Node("C");
		Node nodeD = new Node("D");
		Node nodeE = new Node("E");
		Node nodeF = new Node("F");
		Node nodeG = new Node("G");
		Node nodeH = new Node("H");
		Node nodeI = new Node("I");
		Node nodeJ = new Node("J");
		Node nodeK = new Node("K");
		Node nodeL = new Node("L");
		Node nodeM = new Node("M");
		Node nodeN = new Node("N");
		Node nodeO = new Node("O");
		Node nodeP = new Node("P");
		Node nodeQ = new Node("Q");
		Node nodeR = new Node("R");
		Node nodeS = new Node("S");
		Node nodeT = new Node("T");
		Node nodeU = new Node("U");

		Node node1_0 = new Node("1_0",4);
		Node node1_1 = new Node("1_1",3);
		Node node1_2 = new Node("1_2",5);
		Node node1_3 = new Node("1_3",2);
		Node node1_4 = new Node("1_4",1);
		Node node1_5 = new Node("1_5",4);
		Node node1_6 = new Node("1_6",3);
		Node node1_7 = new Node("1_7",2);
		Node node1_8 = new Node("1_8",5);
		Node node1_9 = new Node("1_9",4);
		Node node2_0 = new Node("2_0",7);
		Node node2_1 = new Node("2_1",3);
		Node node2_2 = new Node("2_2",2);
		Node node2_3 = new Node("2_3",1);
		Node node2_4 = new Node("2_4",4);
		Node node2_5 = new Node("2_5",0);
		Node node2_6 = new Node("2_6",5);
		Node node2_7 = new Node("2_7",3);
		Node node2_8 = new Node("2_8",0);
		Node node2_9 = new Node("2_9",2);
		Node node3_0 = new Node("3_0",7);
		Node node3_1 = new Node("3_1",4);
		Node node3_2 = new Node("3_2",3);
		Node node3_3 = new Node("3_3",6);
		Node node3_4 = new Node("3_4",5);
		Node node3_5 = new Node("3_5",3);
		Node node3_6 = new Node("3_6",1);
		
		root.addChild(nodeB);
		root.addChild(nodeC);
		nodeB.addChild(nodeD);
		nodeB.addChild(nodeE);
		nodeB.addChild(nodeF);
		nodeC.addChild(nodeG);
		nodeC.addChild(nodeH);
		nodeC.addChild(nodeI);
		nodeD.addChild(nodeJ);
		nodeD.addChild(nodeK);
		nodeE.addChild(nodeL);
		nodeF.addChild(nodeM);
		nodeF.addChild(nodeN);
		nodeF.addChild(nodeO);
		nodeG.addChild(nodeP);
		nodeH.addChild(nodeQ);
		nodeH.addChild(nodeR);
		nodeI.addChild(nodeS);
		nodeI.addChild(nodeT);
		nodeI.addChild(nodeU);
		
		nodeJ.addChild(node1_0);
		nodeJ.addChild(node1_1);
		nodeJ.addChild(node1_2);
		
		nodeK.addChild(node1_3);
		nodeK.addChild(node1_4);
		
		nodeL.addChild(node1_5);
		nodeL.addChild(node1_6);
		nodeL.addChild(node1_7);
		
		nodeM.addChild(node1_8);
		nodeM.addChild(node1_9);
		
		nodeN.addChild(node2_0);
		
		nodeO.addChild(node2_1);
		nodeO.addChild(node2_2);
		
		nodeP.addChild(node2_3);
		nodeP.addChild(node2_4);
		nodeP.addChild(node2_5);
		
		nodeQ.addChild(node2_6);
		nodeQ.addChild(node2_7);
		
		nodeR.addChild(node2_8);
		
		nodeS.addChild(node2_9);
		nodeS.addChild(node3_0);
		nodeS.addChild(node3_1);
		
		nodeT.addChild(node3_2);
		nodeT.addChild(node3_3);
		
		nodeU.addChild(node3_4);
		nodeU.addChild(node3_5);
		nodeU.addChild(node3_6);
		
		
		ISearchAlgo algo = new AlphaBetaSearchAlgo();
		algo.execute(root);
		System.out.println("---------------------------");
		ISearchAlgo algoTask3 = new AlphaBetaRighttoLeft();
		algoTask3.execute(root);
		
		
		
		
		
		

	}
}
