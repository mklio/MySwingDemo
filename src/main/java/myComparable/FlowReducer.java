package myComparable;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
	
	FlowBean sumFlow = new FlowBean();

	@Override
	protected void reduce(FlowBean key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
		for (Text value : values) {
			context.write(value, key);
		}
	}

}
