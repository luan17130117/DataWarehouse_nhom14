package puzzle;

import java.util.List;

public class Test {

	public static void main(String[] args) {
		Puzzle p = new Puzzle();
		p.readInput("txt/PuzzleMap.txt", "txt/PuzzleGoalState.txt");

//		System.out.println(p.getInitialState());
//
//		List<Node> children = p.getSuccessors(p.getInitialState());

		// System.out.println(p.heuristic(p.getInitialState(), p.getGoalState()));
		// System.out.println(children.size());
//		for (Node child : children) {
//			System.out.println(child);
//		}
	}
}
