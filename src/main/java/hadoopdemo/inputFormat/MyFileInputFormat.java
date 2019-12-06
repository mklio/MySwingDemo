package hadoopdemo.inputFormat;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.Text;

public class MyFileInputFormat extends FileInputFormat<Text, BytesWritable> {

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// TODO Auto-generated method stub
		return super.isSplitable(context, filename);
	}

	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new MyRecordReader();
	}

}
