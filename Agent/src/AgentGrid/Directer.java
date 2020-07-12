package AgentGrid;

public class Directer {
	public static final int[] LEFT = { -1, 0 };
	public static final int[] RIGHT = { 1, 0 };
	public static final int[] UP = { 0, -1 };
	public static final int[] DOWN = { 0, 1 };

	public static int[] chooseDirection() {
		int direction = (int) (Math.random() * 4) + 1;
		switch (direction) {
		case 1:
			return LEFT;
		case 2:
			return RIGHT;
		case 3:
			return UP;
		case 4:
			return DOWN;
		default:
			return DOWN;
		}
	}
}
