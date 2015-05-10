package mappers;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SamplingMapper extends Mapper<Text, Text, Text, LongWritable> {

	private LongWritable one = new LongWritable(1);
	
	@Override
	protected void map(Text node, Text value,
			Mapper<Text, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		context.write(node, one);
	}

}
