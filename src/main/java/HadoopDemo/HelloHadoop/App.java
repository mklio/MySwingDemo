package HadoopDemo.HelloHadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class App {
	public static void main(String[] args) {
		App app = new App();
		try {
			app.createFile();
		} catch (Exception e) {
			System.out.println("Exception!!!");

		}
	}

	public void createFile() throws Exception {
		System.out.println("Hello createFile!");

		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop:9000"), configuration, "root");
		FSDataOutputStream outputStream = null;
		//outputStream = fs.create(new Path("/test/test01.txt"));
		//outputStream.writeChars("123");
		outputStream.close();
		fs.close();
	}
}
