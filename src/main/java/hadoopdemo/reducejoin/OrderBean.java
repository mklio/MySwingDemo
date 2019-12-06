package hadoopdemo.reducejoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class OrderBean implements WritableComparable<OrderBean>{
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public String getPname() {
		return pname;
	}

	public void setPname(String pname) {
		this.pname = pname;
	}

	private String id;
	private String pid;
	private int amount;
	private String pname;

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return id + "\t" + pname + "\t" + amount;
	}

	@Override
	public int compareTo(OrderBean o) {
		// TODO Auto-generated method stub
		int compare = this.pid.compareTo(o.pid);
		if(compare == 0) {
			return o.pname.compareTo(this.pname);
		} else {
			return compare;
		}
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.id = arg0.readUTF();
		this.pid = arg0.readUTF();
		this.amount = arg0.readInt();
		this.pname = arg0.readUTF();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeUTF(id);
		arg0.writeUTF(pid);
		arg0.writeInt(amount);
		arg0.writeUTF(pname);
	}

}
