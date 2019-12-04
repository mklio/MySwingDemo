package groupingcomparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class OrderBean implements WritableComparable<OrderBean>{
	
	private String orderId;
	private String productId;
	private double price;

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return orderId + "\t" + productId + "\t" + price;
	}

	public String getOrderId() {
		return orderId;
	}

	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}

	public String getProductId() {
		return productId;
	}

	public void setProductId(String productId) {
		this.productId = productId;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.orderId = arg0.readUTF();
		this.productId = arg0.readUTF();
		this.price = arg0.readDouble();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeUTF(orderId);
		arg0.writeUTF(productId);
		arg0.writeDouble(price);
	}

	@Override
	public int compareTo(OrderBean o) {
		// TODO Auto-generated method stub
		int compare = this.orderId.compareTo(o.orderId);
		if(compare == 0) {
			return Double.compare(o.price, this.price);
		} else {
			return compare;
		}
	}

}
