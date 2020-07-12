package puzzle;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class Puzzle {
	public static final int MAX_ROW = 3;// 3x3: Dimension of the puzzle map
	public static final int MAX_COL = 3;

	private Node initialState;
	private Node goalState;

	public Puzzle() {
		this.initialState = new Node(MAX_ROW, MAX_COL);
		this.goalState = new Node(MAX_ROW, MAX_COL);
	}

	public void readInput(final String INITIAL_STATE_MAP_PATH, final String GOAL_STATE_MAP_PATH) {
		try {
			// 1 - Import map
			BufferedReader bufferedReader = new BufferedReader(new FileReader(INITIAL_STATE_MAP_PATH));

			String line = null;
			int row = 0;
			while ((line = bufferedReader.readLine()) != null) {
				String[] tile = line.split(" ");
				for (int col = 0; col < tile.length; col++) {
					initialState.state[row][col] = Integer.parseInt(tile[col]);
				}
				row++;
			}

			bufferedReader.close();

			// 2 - Import goal state
			bufferedReader = new BufferedReader(new FileReader(GOAL_STATE_MAP_PATH));

			line = null;
			row = 0;
			while ((line = bufferedReader.readLine()) != null) {
				String[] tile = line.split(" ");
				for (int col = 0; col < tile.length; col++) {
					goalState.state[row][col] = Integer.parseInt(tile[col]);
				}
				row++;
			}

			bufferedReader.close();

			// 3 - Compute heuristic value and get white tile position
			int[] whiteTilePosition = initialState.getLocation(0);
			System.arraycopy(whiteTilePosition, 0, initialState.whiteTilePosition, 0, whiteTilePosition.length);
			initialState.setH(computeH(initialState, goalState));
			initialState.setG(computeG(initialState, goalState));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public int[] getTilePosition(int[][] state, int tile) {
		// Return position (in 2D) of a tile in current state
		int[] output = new int[2];

		/* Enter your code here */

		return output;
	}

	// distance between P1(x1, y1) and P2(x2, y2)
	public int manhattanDistance(int[] current, final int[] target) {
		return Math.abs(target[0] - current[0]) + Math.abs(target[1] - current[1]);
	}

	// Using manhattanDistance above to compute H
	public int computeH(final Node currentState, final Node goalState) {
		int output = 0;

		for (int i = 0; i < currentState.state.length; i++) {
			for (int j = 0; j < currentState.state[i].length; j++) {
				Integer tile = currentState.state[i][j];
				if (tile != 0) {
					int[] current = { i, j };
					int[] target = goalState.getLocation(tile);
					int manhattanDistance = manhattanDistance(current, target);
					output += manhattanDistance;
				}
			}
		}
		return output;
	}

	public int computeG(final Node currentState, final Node goalState) {
		int output = 0;
		for (int i = 0; i < currentState.state.length; i++) {
			for (int j = 0; i < currentState.state.length; j++) {
				int currentTile = currentState.state[i][j];
//				int goaltile = currentState.state[i][j];
				if (currentTile != 0) {
					if (currentState.state[i][j] != goalState.state[i][j]) {
						output++;
					}

				}
			}
		}
		return output;
	}

	public Node moveTile(final Node state, int[] tile, char operator) {
		Node output = new Node(state);

		if (operator == 'u') {// Case-1: Move tile UP
			// New postion of tile if move UP
			int row = tile[0] - 1;
			int col = tile[1];
			if (row >= 0) {// Tile stands inside the map
				int tmp = state.state[row][col];
				output.state[row][col] = 0;
				output.state[tile[0]][tile[1]] = tmp;
				int[] newWhiteTilePosition = { row, col };
				System.arraycopy(newWhiteTilePosition, 0, output.whiteTilePosition, 0, newWhiteTilePosition.length);
				output.h = computeH(output, goalState);
				return output;
			}
		}

		if (operator == 'd') {// Case-2: Move tile DOWN
			int row = tile[0] + 1;
			int col = tile[1];
			if (row >= 0) {// Tile stands inside the map
				int tmp = state.state[row][col];
				output.state[row][col] = 0;
				output.state[tile[0]][tile[1]] = tmp;
				int[] newWhiteTilePosition = { row, col };
				System.arraycopy(newWhiteTilePosition, 0, output.whiteTilePosition, 0, newWhiteTilePosition.length);
				output.h = computeH(output, goalState);
				return output;
			}

		}

		if (operator == 'l') {// Case-3: Move tile LEFT
			int row = tile[1];
			int col = tile[0] - 1;
			if (row >= 0) {// Tile stands inside the map
				int tmp = state.state[row][col];
				output.state[row][col] = 0;
				output.state[tile[1]][tile[0]] = tmp;
				int[] newWhiteTilePosition = { row, col };
				System.arraycopy(newWhiteTilePosition, 0, output.whiteTilePosition, 0, newWhiteTilePosition.length);
				output.h = computeH(output, goalState);
				return output;
			}

		}

		if (operator == 'r') {// Case-4: Move tile RIGHT
			int row = tile[1];
			int col = tile[0] + 1;
			if (row >= 0) {// Tile stands inside the map
				int tmp = state.state[row][col];
				output.state[row][col] = 0;
				output.state[tile[1]][tile[0]] = tmp;
				int[] newWhiteTilePosition = { row, col };
				System.arraycopy(newWhiteTilePosition, 0, output.whiteTilePosition, 0, newWhiteTilePosition.length);
				output.h = computeH(output, goalState);
				return output;
			}

		}
		return null;
	}

	public ArrayList<Node> getSuccessors(final Node currentState) {
		ArrayList<Node> output = new ArrayList<Node>();
		char[] operators = { 'l', 'r', 'u', 'd' };

		for (char operator : operators) {
			Node tmp = moveTile(currentState, currentState.whiteTilePosition, operator);
			if (tmp != null) {
				output.add(tmp);
			}
		}

		return output;
	}

	public Node getInitialState() {
		return initialState;
	}

	public Node getGoalState() {
		return goalState;
	}
}
