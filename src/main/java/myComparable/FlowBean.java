package myComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean> {
	private long upFlow;
	private long downFlow;
	private long sumFlow;

	public FlowBean() {
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return upFlow + "\t" + downFlow + "\t" + sumFlow;
	}

	public void set(long upFlow, long downFlow) {
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.upFlow  = arg0.readLong();
		this.downFlow = arg0.readLong();
		this.sumFlow = arg0.readLong();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeLong(upFlow);
		arg0.writeLong(downFlow);
		arg0.writeLong(sumFlow);
	}

	@Override
	public int compareTo(FlowBean o) {
		// TODO Auto-generated method stub
		return Long.compare(o.sumFlow, this.sumFlow);
	}
	
	

}
