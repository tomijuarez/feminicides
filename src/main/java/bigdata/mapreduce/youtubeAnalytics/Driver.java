package bigdata.mapreduce.youtubeAnalytics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {

	private static final String jobName = "YoutubeAnalytics";
	private Path inputPath;
	private Path outputPath;
	private Job job;
	
	public Driver(String input, String output) throws Exception {
		this.inputPath = new Path(input);
		this.outputPath = new Path(output);
		this.job = Job.getInstance(new Configuration(), Driver.jobName);
	}
	
	public void resolve() throws Exception {
		//Output pais are <TEXT, INTWRITABLE> typed.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//Set Mapper and Reducer
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		//Format of input file (by lines)
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//Set HDFS paths
		FileInputFormat.addInputPath(this.job, this.inputPath);
		FileOutputFormat.setOutputPath(this.job, this.outputPath);
		
		job.waitForCompletion(true);
	}
}