package bigdata.mapreduce.femicidios;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {
	
	private static final String jobName = "femicides";
	private Path inputPath;
	private Path outputPath;
	private Job job;
	
	public Driver(String input, String output) throws IOException {
		this.inputPath = new Path(input);
		this.outputPath = new Path(output);
		this.job = Job.getInstance(new Configuration(), Driver.jobName);
	}
	
	public void resolve() throws Exception {
		//Add output key and value types.
		this.job.setOutputKeyClass(Text.class);
		this.job.setOutputValueClass(IntWritable.class);
		//Set custom Mapper and Reducer
		this.job.setMapperClass(Map.class);
		this.job.setReducerClass(Reduce.class);
		//Format input and output.
		this.job.setInputFormatClass(TextInputFormat.class);
		this.job.setOutputFormatClass(TextOutputFormat.class);
		//Format input
		FileInputFormat.addInputPath(this.job, this.inputPath);
		//Set output path
		FileOutputFormat.setOutputPath(this.job, this.outputPath);
		//Start work and wait.
		this.job.waitForCompletion(true);
	}
}
