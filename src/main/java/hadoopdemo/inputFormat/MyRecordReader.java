package hadoopdemo.inputFormat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MyRecordReader extends RecordReader<Text, BytesWritable> {

	private boolean notRead = true;
	private Text key;
	private BytesWritable value;
	private FSDataInputStream inputStream;
	private FileSplit fs;

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		IOUtils.closeStream(inputStream);
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return notRead ? 0 : 1;
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		fs = (FileSplit) arg0;
		Path path = fs.getPath();
		FileSystem filesystem = path.getFileSystem(arg1.getConfiguration());
		inputStream = filesystem.open(path);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (notRead) {
			key.set(fs.getPath().toString());
			byte[] buf = new byte[(int) fs.getLength()];
			inputStream.read(buf);
			value.set(buf, 0, buf.length);

			notRead = false;
			return true;
		} else {
			return false;
		}
	}

}
