package Task2Tanh;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         ;

public class Environment {
	public static final Action MOVE_LEFT = new DynamicAction("LEFT");
	public static final Action MOVE_RIGHT = new DynamicAction("RIGHT");
	public static final Action MOVE_UP = new DynamicAction("UP");
	public static final Action MOVE_DOWN = new DynamicAction("DOWN");
	public static final Action SUCK_DIRT = new DynamicAction("SUCK");
	private int score = 0;

	public enum LocationState {
		CLEAN, DIRTY, OBSTAClES
	}

	private EnvironmentState envState;
	private boolean isDone = false;// all squares are CLEAN
	private Agent agent = null;

	public Environment(int m, int n, double dirtRate, double wallRate) {
		envState = new EnvironmentState(m, n, dirtRate, wallRate);
	}

	// add an agent into the environment
	public void addAgent(Agent agent, Position location) {
		this.agent = agent;
		this.envState.setAgentLocation(location);
	}

	public EnvironmentState getCurrentState() {
		return this.envState;
	}

	// Update environment state when agent do an action
	public EnvironmentState executeAction(Action action) {
		Percept percept = getPerceptSeenBy();
		if (action.equals(SUCK_DIRT)) {
			envState.setLocationState(percept.getAgentLocation(), LocationState.CLEAN);
			envState.decreaseDirt();
			score += 500;
			return envState;
		} else if (action.equals(MOVE_RIGHT) && canMove(Directer.RIGHT)) {
			envState.moveAgent(Directer.RIGHT);
		} else if (action.equals(MOVE_LEFT) && canMove(Directer.LEFT)) {
			envState.moveAgent(Directer.LEFT);
		} else if (action.equals(MOVE_UP) && canMove(Directer.UP)) {
			envState.moveAgent(Directer.UP);
		} else if (action.equals(MOVE_DOWN) && canMove(Directer.DOWN)) {
			envState.moveAgent(Directer.DOWN);
		} else {
			score -= 100;
			return envState;
		}
		score -= 10;
		return envState;
	}

	private boolean canMove(int[] stepper) {
		LocationState nextState = envState.nextState(stepper);
		return (nextState != null && nextState != LocationState.OBSTAClES);
	}

	public Percept getPerceptSeenBy() {
		Position agentLocation = envState.getAgentLocation();
		return new Percept(agentLocation, envState.getLocationState(agentLocation));
	}

	public void step() {
		Action anAction = agent.execute(getPerceptSeenBy());
		executeAction(anAction);
		if (envState.isClean())
			isDone = true;
	}

	public void step(int n) {
		for (int i = 0; i < n; i++) {
			step();
		}
	}

	public void stepUntilDone() {
		int i = 0;

		while (!isDone) {
			System.out.println("step: " + i++);
			step();
		}
	}

	public boolean isDone() {
		return isDone;
	}

	public int getScore() {
		return this.score;
	}
}
