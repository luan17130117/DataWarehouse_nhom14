package AgentGrid;

import AgentGrid.Environment.LocationState;

public class AgentProgram {

	public Action execute(Percept p) {// location, status
		if (p.getLocationState() == LocationState.DIRTY)
			return Environment.SUCK_DIRT;
		else {
			int direction = (int) (Math.random() * 4) + 1;
			switch (direction) {
			case 1:
				return Environment.MOVE_LEFT;
			case 2:
				return Environment.MOVE_RIGHT;
			case 3:
				return Environment.MOVE_UP;
			case 4:
				return Environment.MOVE_DOWN;
			}
		}
		return NoOpAction.NO_OP;

	}
}