package hadoopdemo.reducejoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class RJMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>{

	private OrderBean orderBean = new OrderBean();
	private String filename;
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] fields = value.toString().split("\t");
		if(filename.equals("file1")) {
			orderBean.setId(fields[0]);
			orderBean.setPid(fields[1]);
			orderBean.setAmount(Integer.parseInt(fields[2]));
			orderBean.setPname("");
		} else {
			orderBean.setId("");
			orderBean.setPid(fields[0]);
			orderBean.setAmount(0);
			orderBean.setPname(fields[1]);
		}
		context.write(orderBean, NullWritable.get());
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		FileSplit fs = (FileSplit) context.getInputSplit();
		filename = fs.getPath().getName();
	}
	

}
