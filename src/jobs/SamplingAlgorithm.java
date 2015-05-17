package jobs;

import input.GraphReader;
import mappers.SamplingMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

import vos.Graph;
import vos.MMS;
import writers.AlternativeMMSWriter;

public class SamplingAlgorithm {
	
	public static void main(String[] args) throws Exception  {
		Configuration conf = new Configuration();
		conf.set("com.wildfire.graph_file_path", args[0]);
		conf.set("com.wildfire.alternative_mms_file_path", args[1]);
		
		Job job = Job.getInstance(conf, "controllability sampling algorithm");
		

		// First, we read the graph and get the original MMS
		Graph graph = GraphReader.read(job);
		MMS mms = graph.getMMS();
		AlternativeMMSWriter.write(mms, job);
		job.setJarByClass(SamplingAlgorithm.class);
		job.setMapperClass(SamplingMapper.class);
		job.setCombinerClass(LongSumReducer.class);
		job.setReducerClass(LongSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
