package hadoopdemo.hdfsdemo;

import java.net.URL;

class URLCat{
	private String path;
	
	static {
		URL.setURLStreamHandlerFactory(null);
	}
	
	URLCat(String path){
		this.path = path;
	}
}

public class MyFirstHdfs {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		new URLCat("hdfs://localhost:9000/test1/test01.txt");
		System.out.println("Hello World!");

	}

}
