package puzzle;

import java.util.Comparator;

public class Node {
	public int[][] state;
	public int h;
	public int g;
	// the position of white tile
	public int[] whiteTilePosition = new int[2];

	public Node(int row, int col) {
		this.state = new int[row][col];
	}

	public Node(Node node) {
		this.state = new int[node.state.length][node.state[0].length];
		for (int i = 0; i < node.state.length; i++) {
			for (int j = 0; j < node.state[i].length; j++) {
				state[i][j] = node.state[i][j];
			}
		}
		this.h = node.h;
		System.arraycopy(node.whiteTilePosition, 0, this.whiteTilePosition, 0, this.whiteTilePosition.length);
	}

	public int getF() {
		return this.g + this.h;
	}

	public int getH() {
		return h;
	}

	public void setH(int h) {
		this.h = h;
	}

	public void setG(int g) {
		this.g = g;
	}

	// Get the location of a tile in the board
	public int[] getLocation(int tile) {
		int[] result = new int[2];
		for (int i = 0; i < this.state.length; i++) {
			for (int j = 0; j < this.state[i].length; j++) {
				if (this.state[i][j] == tile) {
					result[0] = i;
					result[1] = j;
				}
			}
		}
		return result;
	}

	// Compare 2 nodes by heuristic values
	public static Comparator<Node> HeuristicComparatorByH = new Comparator<Node>() {

		@Override
		public int compare(Node a, Node b) {
			return a.h - b.h;
		}
	};
	// Compare 2 nodes by F values
	public static Comparator<Node> HeuristicComparatorByF = new Comparator<Node>() {

		@Override
		public int compare(Node a, Node b) {
			return a.getF() - b.getF();
		}
	};

	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();
		for (int i = 0; i < state.length; i++) {
			for (int j = 0; j < state[i].length; j++) {
				output.append(state[i][j] + " ");
			}
			output.append("\n");
		}
		return output.toString();
	}
}