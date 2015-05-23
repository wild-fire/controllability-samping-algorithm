package writers;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;

import vos.MMS;

/**
 * This class writes the file that will be feeded into the Hadoop Job and thus it writes in each line the node to be removed from the MMS on each mapper
 * @author David J. Brenes
 *
 */
public class RemovalNodesWriter {
	
	public static void write(MMS mms, JobContext context) throws IOException, URISyntaxException{

		// First, we open the alternatives file 
		URI mmsURI = new URI(context.getConfiguration().get("com.wildfire.removal_nodes_file_path"));
		Path mmsPath = new Path(mmsURI);
		FSDataOutputStream mmsStream = FileSystem.get(mmsURI, context.getConfiguration()).create(mmsPath);

		BufferedWriter mmsFile = new BufferedWriter(new OutputStreamWriter(mmsStream));
		
		// Now, we get the matched nodes and get ready to obtain the random nodes
		ArrayList<String> matchedNodes = new ArrayList<String>(mms.getMatchedNodes());
		Random random = new Random();
		int matchedSize = matchedNodes.size();
		
		// And now, we get N*log(N) random matched nodes from the MMS and set them to be removed by the mappers
		int n = mms.getGraph().getNumberOfNodes();
		// We save this number into job's configuration  
		double configurationsNumber = Math.ceil(n*Math.log(n));
		context.getConfiguration().setDouble("com.wildfire.configurations_number", configurationsNumber);
		for(int i = 0; i < configurationsNumber; i++) {
			mmsFile.write(matchedNodes.get(random.nextInt(matchedSize)));
			if(i < (n*Math.log(n) - 1)) {
				mmsFile.newLine();
			}
		}
		
		mmsFile.close();
	
	}

}
