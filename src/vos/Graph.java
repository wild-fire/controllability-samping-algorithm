package vos;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataInputStream;

public class Graph {

	private Hashtable<String, HashSet<String>> neighbours = new Hashtable<String, HashSet<String>>();
	private BufferedReader graphFile;
	
	public Graph(FSDataInputStream graphFile) throws IOException {
		this.graphFile = new BufferedReader(new InputStreamReader(graphFile));
		String line;
		
		while((line = this.graphFile.readLine())!= null) {
			String[] lineInfo = line.split("\t");
			if(!this.neighbours.containsKey(lineInfo[0])) {
				this.neighbours.put(lineInfo[0], new HashSet<String>());
			}
			if(!this.neighbours.containsKey(lineInfo[1])) {
				this.neighbours.put(lineInfo[1], new HashSet<String>());
			}
			this.neighbours.get(lineInfo[0]).add(lineInfo[1]);
		}
	}

	public Enumeration<String> getNodes(){
		return this.neighbours.keys();		
	}
	
	public int getNumberOfNodes() {
		return this.neighbours.keySet().size();
	}

	public Iterator<String> getNeighbours(String node){
		return this.neighbours.get(node).iterator();		
	}


}
