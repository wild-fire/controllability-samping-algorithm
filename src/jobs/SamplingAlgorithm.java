package jobs;

import input.GraphInputFormat;
import mappers.SamplingMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

public class SamplingAlgorithm {
	
	public static void main(String[] args) throws Exception  {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "controllability sampling algorithm");
		job.setJarByClass(SamplingAlgorithm.class);
		job.setInputFormatClass(GraphInputFormat.class);
		job.setMapperClass(SamplingMapper.class);
		job.setCombinerClass(LongSumReducer.class);
		job.setReducerClass(LongSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
