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
 * This class writes the file that will be feeded into the Hadoop Job and thus it writes in each line all the information the mapper will need: The current MMS and the node to be removed
 * @author David J. Brenes
 *
 */
public class AlternativeMMSWriter {
	
	public static void write(MMS mms, JobContext context) throws IOException, URISyntaxException{

		// First, we open the alternatives file 
		URI mmsURI = new URI(context.getConfiguration().get("com.wildfire.alternative_mms_file_path"));
		Path mmsPath = new Path(mmsURI);
		FSDataOutputStream mmsStream = FileSystem.get(mmsURI, context.getConfiguration()).create(mmsPath);

		BufferedWriter mmsFile = new BufferedWriter(new OutputStreamWriter(mmsStream));
		
		// Now we build the string for the edges		
		StringBuilder edgesString = new StringBuilder();
		
		for(String source : Collections.list(mms.getEdges().keys())) {
			edgesString.append(source);
			edgesString.append(',');
			edgesString.append(mms.getEdges().get(source));
			edgesString.append(';');
		}
		
		// Now, we get the matched nodes and get ready to obtain the random nodes
		ArrayList<String> matchedNodes = new ArrayList<String>(mms.getMatchedNodes());
		Random random = new Random();
		int matchedSize = matchedNodes.size();
		
		// And now, we get N*log(N) random matched nodes from the MMS and set them to be removed by the mappers
		int n = mms.getGraph().getNumberOfNodes();
		for(int i = 0; i < n*Math.log(n); i++) {
			mmsFile.write(matchedNodes.get(random.nextInt(matchedSize)));
			mmsFile.write('-');
			mmsFile.write(edgesString.toString());
			if(i < (n*Math.log(n) - 1)) {
				mmsFile.newLine();
			}
		}
		
		mmsFile.close();
	
	}

}
