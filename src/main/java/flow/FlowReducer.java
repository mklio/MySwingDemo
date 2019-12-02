package flow;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
	
	FlowBean sumFlow = new FlowBean();

	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context)throws IOException, InterruptedException {
		long sum_upFlow = 0;
		long sum_downFlow = 0;
		for (FlowBean value : values) {
			sum_upFlow += value.getUpFlow();
			sum_downFlow += value.getDownFlow();
		}
		sumFlow.set(sum_upFlow, sum_downFlow);
		context.write(key, sumFlow);
	}

}
