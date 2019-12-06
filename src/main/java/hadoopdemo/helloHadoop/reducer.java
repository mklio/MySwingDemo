package hadoopdemo.helloHadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	private IntWritable total = new IntWritable();

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int sum = 0;
		for(IntWritable value : arg1) {
			sum+=value.get();
		}
		total.set(sum);
		arg2.write(arg0, total);
	}

}
