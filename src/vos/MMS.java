package vos;

import java.util.Dictionary;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

/**
 * This class represents a Maximum Matching Set storing those nodes that are matched, those that aren't and all the edges.
 * @author David J. Brenes
 *
 */
public class MMS {
	/**
	 * Set of matched nodes. Those that are target in any of the edges
	 */
	private Set<String> matchedNodes = new HashSet<String>();
	/**
	 * Set of unmatched nodes. Those that are never target of any edge. These are the nodes who control the graph.  
	 */
	private Set<String> unmatchedNodes = new HashSet<String>();
	/**
	 * Dictionary of edges, so we can travel from sources to targets in the augmenting path algorithm.
	 */
	private Dictionary<String, String> edges = new Hashtable<String, String>();
	/**
	 * Reverse dictionary of edges, so we can travel from targets to sources in the augmenting path algorithm.
	 */
	private Dictionary<String, String> reverseEdges = new Hashtable<String, String>();
	/**
	 * The graph for which this MMS is a Maximum Matching Set
	 */
	private Graph graph;
	
	public MMS(Graph graph) {
		this.graph = graph;
	}

	public Graph getGraph() {
		return graph;
	}

	/**
	 * This methods adds a matching edge to the MMS. It also fills the matched and unmatched nodes set
	 * @param source The source of the edge, the matching node
	 * @param target The target of the edge, the matched node
	 */
	public void addEdge(String source, String target) {
		// First, if the source is not a matched node then we add it to the unmatched ones
		if(!isMatched(source)) {
			this.unmatchedNodes.add(source);
		}
		// Then, if the target was an unmatched node, then we remove it, as it's matched now 
		if (this.unmatchedNodes.contains(target)) {
			this.unmatchedNodes.remove(target);
		}
		// Then, adding the target to the matched ones
		this.unmatchedNodes.add(target);
		
		// And finally we add the edges (both the normal and the reverse one).
		this.edges.put(source, target);
		this.reverseEdges.put(source, target);
		
	}
	
	public Set<String> getMatchedNodes() {
		return matchedNodes;
	}

	public Dictionary<String, String> getEdges() {
		return edges;
	}

	public boolean isMatched(String node) {
		return this.matchedNodes.contains(node);
	}
}
