package input;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;

import vos.Graph;

public class GraphInputSplit extends InputSplit {

	private Graph graph;
	
	public GraphInputSplit(Graph graph) {
		this.graph = graph;
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return 1;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[]{"Just here"};
	}

	public Graph getGraph() {
		return graph;
	}

}
