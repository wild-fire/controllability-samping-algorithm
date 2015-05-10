package readers;

import input.GraphInputSplit;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Iterator;

import org.apache.commons.collections.iterators.EmptyIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import vos.Graph;

public class RemovalNodeReader extends RecordReader<Text, Text> {

	private Graph graph;
	private Text currentNode = new Text();
	private Text currentNeighbour = new Text();
	private Iterator<String> currentNeighbours = EmptyIterator.INSTANCE;
	private Enumeration<String> nodes;
	private int usedNodes = 0;
	
	public RemovalNodeReader(GraphInputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.initialize(split, context);
		this.graph = split.getGraph();
		this.nodes = this.graph.getNodes();
	}
	
	@Override
	public void close() throws IOException {
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return this.currentNode;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return this.currentNeighbour;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return this.usedNodes / this.graph.getNumberOfNodes();
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(!this.currentNeighbours.hasNext()) {
			if(!this.nodes.hasMoreElements()) {
				return false;
			}
			
			this.currentNode.set(this.nodes.nextElement());
			this.usedNodes++;
			this.currentNeighbours = this.graph.getNeighbours(this.currentNode.toString());
		}
		if(!this.currentNeighbours.hasNext()) {
			return nextKeyValue();
		}
		this.currentNeighbour.set(this.currentNeighbours.next());
		return true;
	}

}
