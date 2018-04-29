package bigdata.mapreduce.youtubeAnalytics;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	@Override
	public void reduce(Text category, Iterable<IntWritable> videosUploaded, Context context) 
			throws IOException, InterruptedException {
		
		int sum = 0;
		while(videosUploaded.iterator().hasNext()) {
			sum += videosUploaded.iterator().next().get();
		}
		context.write(category, new IntWritable(sum));
	}
}
