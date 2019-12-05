package outputFormat;

import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MyRecordWriter extends RecordWriter<LongWritable, Text> {

	private FileOutputStream f1;
	private FileOutputStream f2;

	public void initialize() throws IOException {
		f1 = new FileOutputStream("D:\\test1\\");
		f2 = new FileOutputStream("D:\\test2\\");
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
