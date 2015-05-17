package readers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;

import vos.Graph;

public class GraphReader {

	public static Graph read(JobContext context) throws IOException, URISyntaxException{

		URI graphURI = new URI(context.getConfiguration().get("com.wildfire.graph_file_path"));
		Path graphPath = new Path(graphURI);
		FSDataInputStream graphStream = FileSystem.get(graphURI, context.getConfiguration()).open(graphPath);
		
		Graph graph = new Graph();
		 
		BufferedReader graphFile = new BufferedReader(new InputStreamReader(graphStream));
		String line;
		
		while((line = graphFile.readLine())!= null) {
			String[] lineInfo = line.split("\t");
			// The edge is reversed because in our graph file A -> B means A mentions B and we are actually interested in the opposite (i.e. B influences A) 
			graph.addEdge(lineInfo[1], lineInfo[0]);
		}
		
		return graph;		
		
	}
	
	

}
