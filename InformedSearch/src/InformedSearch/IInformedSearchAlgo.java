package InformedSearch;

public interface IInformedSearchAlgo {
	public Node execute(Node tree, String goal);

	public Node execute(Node tree, String start, String goal);
}
