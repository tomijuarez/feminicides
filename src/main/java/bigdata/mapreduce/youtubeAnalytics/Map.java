package bigdata.mapreduce.youtubeAnalytics;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final String separator = "\t";
	private IntWritable value = new IntWritable(1);
	private Text key = new Text();
	
	@Override
	public void map(LongWritable offset, Text row, Context context) 
			throws IOException, InterruptedException {
		
		//Split line by tabs.
		String chunks[] = row.toString().split(Map.separator);
		//We need only the category of the video as a key (position 3 starting from 0).
		//As a value, we will start with 1 as its value.
		if (chunks.length > 3) {
			this.key.set(chunks[3]);
			context.write(this.key, this.value);
		}
	}
}
