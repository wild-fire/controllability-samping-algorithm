package vos;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import readers.GraphReader;

public class MMSTest {

	public final static Random random = new Random();

	@Test
	public void testRemoveNode() throws FileNotFoundException, IOException, URISyntaxException {
		Graph graph = GraphReader.read(new BufferedReader(new FileReader(
				"fixtures/graph-tiny")));
		
		MMS mms = graph.getMMS();
		

		ArrayList<String> matchedNodes = new ArrayList<String>(mms.getMatchedNodes());
		String nodeToRemove = matchedNodes.get(random.nextInt(matchedNodes.size()));
		
		Boolean worked = mms.removeNode(nodeToRemove);
		
		HashSet<String> nodes = new HashSet<String>();
		nodes.addAll(mms.getMatchedNodes());
		nodes.addAll(mms.getUnmatchedNodes());
		
		HashSet<String> graphNodes = new HashSet<String>(Collections.list(graph.getNodes()));
		
		Assert.assertEquals(nodes, graphNodes);
		
		Enumeration<String> targets = mms.getEdges().elements();
		
		while(targets.hasMoreElements()) {
			Assert.assertThat(mms.getMatchedNodes(), org.hamcrest.CoreMatchers.hasItem(targets.nextElement()) );
		}

	}

}
