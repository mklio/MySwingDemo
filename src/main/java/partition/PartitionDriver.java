package partition;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import flow.FlowMapper;
import flow.FlowReducer;

public class PartitionDriver{
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(PartitionDriver.class);
		
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(5);
		job.setPartitionerClass(MyPartition.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean b = job.waitForCompletion(true);
		System.exit(b ? 0 : 1);
	}
}
