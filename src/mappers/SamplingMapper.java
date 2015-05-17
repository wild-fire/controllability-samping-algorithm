package mappers;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import readers.GraphReader;
import readers.MMSReader;
import vos.Graph;
import vos.MMS;


public class SamplingMapper extends Mapper<Object, Text, Text, LongWritable> {

	private LongWritable one = new LongWritable(1);
	private Text unmatchedNode = new Text();
	private Graph graph;
	private MMS mms;
	
	@SuppressWarnings("unchecked")
	@Override
	protected void map(Object nothing, Text removalNode,
			Mapper<Object, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// we must read the MMS and Graph file each time as we modify them each mapping run 
		try {
			this.graph = GraphReader.read(context);
			this.mms = MMSReader.read(this.graph, context);
		} catch (URISyntaxException e) {
			throw new IOException(e.getMessage());
		}
		
		// We prepare the array where we are going to save the configurations of unmatched nodes
		ArrayList<Set<String>> alternativeConfigurations = new ArrayList<Set<String>>();
		// And the variable to get a random one 
		Random random = new Random();
		
		// And prepare the first removal node (the one decided randomly by the RemovalNodesWriter and delivered by Hadoop) 
		String nodeToRemove = removalNode.toString();
		
		System.out.println(">> " + nodeToRemove);
		System.out.println(">>>> " + this.mms.getUnmatchedNodes());
		
		// While we can remove nodes
		while(this.mms.removeNode(nodeToRemove)) {
			// We store the unmatched nodes as a driver node configuration
			alternativeConfigurations.add((Set<String>) this.mms.getUnmatchedNodes().clone());
			System.out.println(">>>> " + this.mms.getUnmatchedNodes());
			// And we get the matched nodes to obtain the next random removal node
			ArrayList<String> matchedNodes = new ArrayList<String>(mms.getMatchedNodes());
			nodeToRemove = matchedNodes.get(random.nextInt(matchedNodes.size()));
		}
		
		// Now we get a random alternative configuration (if there's any)
		if(!alternativeConfigurations.isEmpty()) {
			for(String unmatchedNode : alternativeConfigurations.get(random.nextInt(alternativeConfigurations.size()))) {
				System.out.println(">>>>>> " + unmatchedNode);
				this.unmatchedNode.set(unmatchedNode);
				context.write(this.unmatchedNode, one);
			}
		}
	}

}
