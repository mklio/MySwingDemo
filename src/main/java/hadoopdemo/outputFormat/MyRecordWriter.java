package hadoopdemo.outputFormat;

import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyRecordWriter extends RecordWriter<LongWritable, Text> {

	private FSDataOutputStream f1;
	private FSDataOutputStream f2;

	public void initialize(TaskAttemptContext job) throws IOException {
		String outdir = job.getConfiguration().get(FileOutputFormat.OUTDIR);
		FileSystem fs = FileSystem.get(job.getConfiguration());
		f1 = fs.create(new Path("/test_f1"));
		f2 = fs.create(new Path("/test_f2"));
	}

	@Override
	public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		IOUtils.closeStream(f1);
		IOUtils.closeStream(f2);
	}

	@Override
	public void write(LongWritable arg0, Text arg1) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String out = arg1.toString() + "\n";
		if (out.contains("f1")) {
			f1.write(out.getBytes());
		} else {
			f2.write(out.getBytes());
		}
	}

}
