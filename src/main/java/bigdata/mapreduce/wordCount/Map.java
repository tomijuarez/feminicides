package bigdata.mapreduce.wordCount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This mapper takes <LongWritable, Text> key-value pairs from input.
 *  and returns <Text, IntWritable> key-value pairs as a intermediate
 *  output.
 *  
 * @author Tomi
 *
 */

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static String separator = " ";
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		//Get the whole line and tokenize by space separator.
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line, Map.separator);
		//For each token, generate its pair.
		//The value will be 1, because the counting task will be done in Reduce layer.
		while(st.hasMoreTokens()) {
			this.word.set(st.nextToken());
			//Write into HDFS aka context for communicating with Reducer.
			context.write(this.word, this.one);
		}
	}
}