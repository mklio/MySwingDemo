package hadoopdemo.groupingcomparator;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable>{

	@Override
	protected void reduce(OrderBean arg0, Iterable<NullWritable> arg1,
			Reducer<OrderBean, NullWritable, OrderBean, NullWritable>.Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		arg2.write(arg0, NullWritable.get());
	}

}
