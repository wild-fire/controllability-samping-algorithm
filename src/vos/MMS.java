package vos;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Dictionary;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

/**
 * This class represents a Maximum Matching Set storing those nodes that are matched, those that aren't and all the edges.
 * @author David J. Brenes
 *
 */
public class MMS implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7217771981456337166L;
	/**
	 * Set of matched nodes. Those that are target in any of the edges
	 */
	private HashSet<String> matchedNodes = new HashSet<String>();
	/**
	 * Set of unmatched nodes. Those that are never target of any edge. These are the nodes who control the graph.  
	 */
	private HashSet<String> unmatchedNodes = new HashSet<String>();
	/**
	 * Dictionary of edges, so we can travel from sources to targets in the augmenting path algorithm.
	 */
	private Hashtable<String, String> edges = new Hashtable<String, String>();
	/**
	 * Reverse dictionary of edges, so we can travel from targets to sources in the augmenting path algorithm.
	 */
	private Hashtable<String, String> reverseEdges = new Hashtable<String, String>();
	/**
	 * The graph for which this MMS is a Maximum Matching Set
	 */
	private Graph graph;
	
	public MMS(Graph graph) {
		this.setGraph(graph);
		if(this.graph != null) {
			this.unmatchedNodes.addAll(Collections.list(this.graph.getNodes()));
		}
	}


	public Graph getGraph() {
		return graph;
	}

	public void setGraph(Graph g) {
		this.graph = g;
	}
	
	@SuppressWarnings("unchecked")
	public MMS clone() {
		MMS clonedMMS = new MMS(this.graph);
		clonedMMS.matchedNodes = (HashSet<String>) this.matchedNodes.clone();
		clonedMMS.unmatchedNodes = (HashSet<String>) this.unmatchedNodes.clone();
		clonedMMS.edges = (Hashtable<String, String>) this.edges.clone();
		clonedMMS.reverseEdges = (Hashtable<String, String>) this.reverseEdges.clone();
		return clonedMMS;
	}
	
	/**
	 * This methods adds a matching edge to the MMS. It also fills the matched and unmatched nodes set
	 * @param source The source of the edge, the matching node
	 * @param target The target of the edge, the matched node
	 */
	public void addEdge(String source, String target) {
		// First, if the target was an unmatched node, then we remove it, as it's matched now 
		if (this.unmatchedNodes.contains(target)) {
			this.unmatchedNodes.remove(target);
		}
		// Then, adding the target to the matched ones
		this.matchedNodes.add(target);
		
		// And finally we add the edges (both the normal and the reverse one).
		this.edges.put(source, target);
		this.reverseEdges.put(target, source);
		
	}
	
	public void removeEdge(String source, String target) {
		// First, if the target was a matched node, then we remove it, as it's unmatched now 
		if (this.matchedNodes.contains(target)) {
			this.matchedNodes.remove(target);
		}
		// Then, adding the target to the unmatched ones
		this.unmatchedNodes.add(target);
		
		// And finally we add the edges (both the normal and the reverse one).
		this.edges.remove(source);
		this.reverseEdges.remove(target);
		
	}
	
	public HashSet<String> getUnmatchedNodes() {
		return unmatchedNodes;
	}

	public HashSet<String> getMatchedNodes() {
		return matchedNodes;
	}

	public Dictionary<String, String> getEdges() {
		return edges;
	}

	public boolean isMatched(String node) {
		return this.matchedNodes.contains(node);
	}
	
	public boolean removeNode(String removalNode) {
	    // We get the out node matching the removal node
	    String outNode = this.reverseEdges.get(removalNode);
	    
	    // We remove the edge from outNode to the removal node
	    this.removeEdge(outNode, removalNode);
	    
	    // We remove every incoming edge to our removal node in the graph
	    // We don't remove the node itself or any outgoing edge as we will need it later
	    this.graph.removeIncomingEdge(removalNode);

	    // We randomize the targets from the out node on the graph
	    ArrayList<String> targets = new ArrayList<String>(this.graph.getNeighbours(outNode));
	    Collections.shuffle(targets);
	    
	    // We mark the removal node as already traversed
    	Set<String> path = new HashSet<String>();
    	path.add(removalNode);
    	
	    // From each target we try to find an augmenting path from the out node to its target
	    for(String target : targets) {
	    	// If we find one, we return true
	    	if(this.findAugmentingPath(outNode, target, path)) {
	    		return true;
	    	}
	    }
	    
	    // If we don't find any we return false
		return false;
	}
	
	/**
	 * This method finds an augmenting path from a node source to a node target. If target is not matched yet then we are done, else it gets recursive to find an augmenting path for the node that matches the target
	 * @param source The source of the augmenting path
	 * @param target The target of the augmenting path
	 * @param path The nodes that have been already traversed, so we don't enter into a cycle
	 * @return
	 */
	public boolean findAugmentingPath(String source, String target, Set<String> path) {
		if(!this.isMatched(target)) {
			this.addEdge(source, target);
			return true;
		} else {
		    // We get the out node matching the target
		    String outNode = this.reverseEdges.get(target);
		    
		    // We remove the edge from outNode to the target
		    this.removeEdge(outNode, target);

		    // We add the edge we want to create
		    this.addEdge(source, target);
		    // We add the target to our path
		    path.add(target);
		    
		    // We get the targets from the out node on the graph
		    ArrayList<String> targets = new ArrayList<String>(this.graph.getNeighbours(outNode));
		    // We remove the nodes on the path
		    targets.removeAll(path);
		    // And randomize the rest of them
		    Collections.shuffle(targets);
		    
		    // Now for each of these targets, we try to find an augmenting path
		    for(String newTarget : targets) {
		    	// If we find one, we return true
		    	if(this.findAugmentingPath(outNode, newTarget, path)) {
		    		return true;
		    	}
		    }
		    
		    // If we didn't find anything, we leave everything as found
		    // First, we remove target from the already traversed path
		    path.remove(target);
		    // Then we remove the edges we wanted to create from source to target
		    this.edges.remove(source);
		    this.reverseEdges.remove(target);
		    // And then we add the removed edge from the out node to target
		    this.addEdge(outNode, target);
		}
		
		// If we reach here it's because we didn't find anything, so move on
		return false;
	}
	
}
