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
 * This class writes a CSV file with one edge of the MMS per line. 
 * @author David J. Brenes
 *
 */
public class MMSWriter {
	
	public static void write(MMS mms, JobContext context) throws IOException, URISyntaxException{

		// First, we open the alternatives file 
		URI mmsURI = new URI(context.getConfiguration().get("com.wildfire.mms_file_path"));
		Path mmsPath = new Path(mmsURI);
		FSDataOutputStream mmsStream = FileSystem.get(mmsURI, context.getConfiguration()).create(mmsPath);

		BufferedWriter mmsFile = new BufferedWriter(new OutputStreamWriter(mmsStream));
		
		// Now we build the string for the edges		
		StringBuilder edgesString = new StringBuilder();
		
		for(String source : Collections.list(mms.getEdges().keys())) {
			mmsFile.write(source + ',' + mms.getEdges().get(source));
			mmsFile.newLine();			
		}
		
		mmsFile.close();
	
	}

}
