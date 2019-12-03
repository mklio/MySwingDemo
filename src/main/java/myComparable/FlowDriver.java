package myComparable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowDriver{
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(FlowDriver.class);
		
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);
		
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean b = job.waitForCompletion(true);
		System.exit(b ? 0 : 1);
	}
}
