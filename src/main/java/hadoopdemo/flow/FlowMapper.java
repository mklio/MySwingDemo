package hadoopdemo.flow;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	FlowBean flow = new FlowBean();
	Text phone = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = line.split("\t");
		phone.set(fields[1]);
		long upFlow = Long.parseLong(fields[fields.length - 3]);
		long downFlow = Long.parseLong(fields[fields.length - 2]);
        flow.set(downFlow, upFlow);
		context.write(phone, flow);
	}

}
