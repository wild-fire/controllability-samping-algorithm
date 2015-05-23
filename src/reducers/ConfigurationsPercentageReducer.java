package reducers;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ConfigurationsPercentageReducer extends Reducer<Text, LongWritable, Text, DoubleWritable> {

	private DoubleWritable result = new DoubleWritable();
	
	public void reduce(Text node, Iterable<LongWritable> counts,
			Reducer<Text, LongWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		long configurationsNumber = 0;
		for( LongWritable item : counts) {
			configurationsNumber += item.get();
		}
		result.set(configurationsNumber/context.getConfiguration().getDouble("com.wildfire.configurations_number", -1));
		context.write(node, result);
	}

}
