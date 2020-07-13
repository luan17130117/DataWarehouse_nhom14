package AgentGrid;

import AgentGrid.Environment.LocationState;

public class EnvironmentState {
	private LocationState[][] floor = null;
	private Position agentLocation = new Position(0, 0);
	private int dirtCells;

	public EnvironmentState(int m, int n, double dirtRate, double wallRate) {
		this.floor = new Environment.LocationState[m][n];
		for (int i = 0; i < floor.length; i++) {
			for (int j = 0; j < floor[i].length; j++) {
				floor[i][j] = Environment.LocationState.CLEAN;
			}
		}
//		floor[0][0] = LocationState.CLEAN;
		init(dirtRate, LocationState.DIRTY);
		init(wallRate, LocationState.OBSTAClES);
		display();
	}

	private void init(double rate, LocationState state) {
		int cells = (int) (floor.length * floor[0].length * rate);
		if (state == LocationState.DIRTY)
			dirtCells = cells;
		for (int i = 0; i < cells; i++) {
			while (true) {
				int x = (int) (Math.random() * floor.length);
				int y = (int) (Math.random() * floor[0].length);
				if (floor[x][y] == LocationState.CLEAN) {
					floor[x][y] = state;
					break;
				}
			}
		}
	}

	public boolean isClean() {
		return dirtCells == 0;
	}

	public void decreaseDirt() {
		this.dirtCells--;
	}

	public void moveAgent(int[] stepper) {
		setAgentLocation(new Position(agentLocation.getX() + stepper[0], agentLocation.getY() + stepper[1]));
	}

	public void setAgentLocation(Position location) {
		this.agentLocation = location;
	}

	public Position getAgentLocation() {
		return this.agentLocation;
	}

	public LocationState getLocationState(Position location) {
		return this.floor[location.getX()][location.getY()];
	}

	public void setLocationState(Position location, LocationState locationState) {
		this.floor[location.getX()][location.getY()] = locationState;
	}

	public String printState() {
		String rs = "";
		for (int i = 0; i < floor.length; i++) {
			for (int j = 0; j < floor[i].length; j++) {
				rs += floor[i][j] + ", ";
			}
			rs += "\r\n" + "\n\t";
		}
		return rs;
	}

	public void display() {
		System.out.println("Environment state: \n\t" + this.printState());
	}

	public LocationState nextState(int[] stepper) {
		try {
			return getLocationState(new Position(agentLocation.getX() + stepper[0], agentLocation.getY() + stepper[1]));
		} catch (Exception e) {
			return null;
		}
	}

	public LocationState[][] getFloor() {
		return floor;
	}

	public void printAgentPostition() {
		System.out.println("Agent: [" + agentLocation.getX() + "," + agentLocation.getY()+"]");
	}

}