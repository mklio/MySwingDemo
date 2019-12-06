package hadoopdemo.reducejoin;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

public class RJComparator extends WritableComparator{
	protected RJComparator() {
		super(OrderBean.class, true);
	}

	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		OrderBean oa = (OrderBean)a;
		OrderBean ob = (OrderBean)b;
		return oa.getPid().compareTo(ob.getPid());
	}

}
