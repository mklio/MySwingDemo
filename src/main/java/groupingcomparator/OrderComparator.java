package groupingcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderComparator extends WritableComparator {
	OrderBean a;
	OrderBean b;
	
	protected OrderComparator(){
		super(OrderBean.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		this.a = (OrderBean)a;
		this.b = (OrderBean)b;
		
		return this.a.getOrderId().compareTo(this.b.getOrderId());
	}

}
