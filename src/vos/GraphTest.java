package vos;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;

import org.junit.Assert;
import org.junit.Test;

import readers.GraphReader;

public class GraphTest {

	@Test
	public void testGetMMS() throws FileNotFoundException, IOException,
			URISyntaxException {
		Graph graph = GraphReader.read(new BufferedReader(new FileReader(
				"fixtures/graph-tiny")));
		
		MMS mms = graph.getMMS();
		
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
