package jobs;

import mappers.SamplingMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

import readers.GraphReader;
import vos.Graph;
import vos.MMS;
import writers.MMSWriter;
import writers.RemovalNodesWriter;

public class SamplingAlgorithm {
	
	public static void main(String[] args) throws Exception  {
		Configuration conf = new Configuration();
		conf.set("com.wildfire.graph_file_path", args[0]);
		conf.set("com.wildfire.mms_file_path", args[1] + "/mms.csv");
		conf.set("com.wildfire.removal_nodes_file_path", args[1] + "/removal-nodes.csv");
		
		Job job = Job.getInstance(conf, "controllability sampling algorithm");
		

		// First, we read the graph and get the original MMS
		Graph graph = GraphReader.read(job);
		MMS mms = graph.getMMS();
		MMSWriter.write(mms, job);
		RemovalNodesWriter.write(mms, job);
		job.setJarByClass(SamplingAlgorithm.class);
		job.setMapperClass(SamplingMapper.class);
		job.setCombinerClass(LongSumReducer.class);
		job.setReducerClass(LongSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
	    FileInputFormat.setInputPaths(job, args[1] + "/removal-nodes.csv");
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
