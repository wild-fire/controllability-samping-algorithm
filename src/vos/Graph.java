package vos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;

public class Graph {

	private Hashtable<String, HashSet<String>> neighbours = new Hashtable<String, HashSet<String>>();

	public Enumeration<String> getNodes(){
		return this.neighbours.keys();		
	}
	
	public int getNumberOfNodes() {
		return this.neighbours.keySet().size();
	}

	public Iterator<String> getNeighbours(String node){
		return this.neighbours.get(node).iterator();		
	}
	
	public void addEdge(String source, String target){
		if(!this.neighbours.containsKey(source)) {
			this.neighbours.put(source, new HashSet<String>());
		}
		if(!this.neighbours.containsKey(target)) {
			this.neighbours.put(target, new HashSet<String>());
		}
		this.neighbours.get(source).add(target);
	}
	
	
	public MMS getMMS() {
		MMS mms = new MMS(this);
		
		for(String source : Collections.list(neighbours.keys()) ) {
			HashSet<String> targets = neighbours.get(source);
			ArrayList<String> unmatchedTargets = new ArrayList<String>();
			
			for(String target: targets) {
				if(!mms.isMatched(target)) {
					unmatchedTargets.add(target);
				}
			}
			if(!unmatchedTargets.isEmpty()) {
				Collections.shuffle(unmatchedTargets);
				mms.addEdge(source, unmatchedTargets.get(0));
			}
		}
		
		return mms;
		
	}


}
