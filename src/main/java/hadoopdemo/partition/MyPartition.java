package hadoopdemo.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import hadoopdemo.flow.FlowBean;

public class MyPartition extends Partitioner<Text, FlowBean>{

	@Override
	public int getPartition(Text arg0, FlowBean arg1, int arg2) {
		// TODO Auto-generated method stub
		String phone = arg0.toString();
		switch(phone.substring(0, 3)) {
		case "136":
			return 0;
		case "137":
			return 1;
		case "138":
			return 2;
		case "139":
			return 3;
		default:
			return 4;
		}
	}

}
