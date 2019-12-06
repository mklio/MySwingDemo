package hadoopdemo.groupingcomparator;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

	private OrderBean orderbean = new OrderBean();
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] fields = value.toString().split("\t");
		orderbean.setOrderId(fields[0]);
		orderbean.setProductId(fields[1]);
		orderbean.setPrice(Double.parseDouble(fields[2]));
		context.write(orderbean, NullWritable.get());
	}

}
