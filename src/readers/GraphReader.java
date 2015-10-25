package readers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;

import org.apache.spark.api.java.JavaSparkContext;

import vos.Graph;

public class GraphReader {

	public static Graph read(JavaSparkContext sparkContext) throws IOException, URISyntaxException{

		Graph graph = new Graph();
		 
		for(String line : sparkContext.textFile(sparkContext.getConf().get("com.wildfire.graph_file_path")).toArray()) {
			String[] lineInfo = line.split("\t");
			// The edge is reversed because in our graph file A -> B means A mentions B and we are actually interested in the opposite (i.e. B influences A) 
			graph.addEdge(lineInfo[1], lineInfo[0]);
		}
		
		return graph;		
		
	}
	
	

}
