package queen;

public class SA_NQueenAlgo implements ILocalSearchAIgo {

	@Override
	public Node excute(Node initialState) {
		double T = 1000.0;
		double coolingRate = 0.995;
		Node current = new Node(initialState);
		while (current.getH() != 0) {
			int currentH = current.getH();
			Node next = current.selectNextRandomCandidate();
			int deltaE = currentH = next.getH();
			if (deltaE > 0) {
				current = new Node(next);
			} else if (Math.exp(deltaE / T) > Math.random()) {
				current = new Node(next);

			}
			T *= coolingRate;

		}
		return current;
	}

}
