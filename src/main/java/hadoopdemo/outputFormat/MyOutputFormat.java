package hadoopdemo.outputFormat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyOutputFormat extends FileOutputFormat<LongWritable, Text> {

	@Override
	public RecordWriter<LongWritable, Text> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		MyRecordWriter my = new MyRecordWriter();
		my.initialize(job);
		return my;
	}

}
