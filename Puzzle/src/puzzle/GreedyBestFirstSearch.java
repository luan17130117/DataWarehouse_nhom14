package puzzle;

import java.util.List;
import java.util.PriorityQueue;

public class GreedyBestFirstSearch implements IPuzzleAlgo{

	@Override
	public Node execute(Puzzle p) {
		PriorityQueue<Node> frontier = new PriorityQueue<Node>(Node.HeuristicComparatorByH); 
		frontier.add(p.getInitialState());
		while (!frontier.isEmpty()) {
			Node currentNode = frontier.poll();
			if(p.computeH(currentNode, p.getGoalState())==0) {
				return currentNode;
			}
			List<Node> children = p.getSuccessors(currentNode);
			for (Node chil : children) {
				chil.setH(p.computeH(currentNode, p.getGoalState()));
				frontier.add(chil);
			}
		}
		return null;
	}

}
