package mappers;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SamplingMapper extends Mapper<Text, Text, Text, LongWritable> {

	@Override
	protected void map(Text key, Text value,
			Mapper<Text, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
	}

}
