package input;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import readers.RemovalNodeReader;
import vos.Graph;

public class GraphInputFormat extends InputFormat<Text, Text> {

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new RemovalNodeReader((GraphInputSplit)split, context);
	}

	@Override
	/**
	 * Here we read and build the graph and calculate the initial maximum matching
	 */
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		ArrayList<InputSplit> inputSplits = new ArrayList<InputSplit>();
		try {
			URI graphURI = new URI(context.getConfiguration().get("com.wildfire.graph_file_path"));
			Path graphPath = new Path(graphURI);
			inputSplits.add(new GraphInputSplit(new Graph(FileSystem.get(graphURI, context.getConfiguration()).open(graphPath))));
		} catch (URISyntaxException e) {
			throw new IOException("Bad graph URI syntax: " + context.getConfiguration().get("com.wildfire.graph_file_path")); 
		}
		
		
		return inputSplits;
	}
	

}
