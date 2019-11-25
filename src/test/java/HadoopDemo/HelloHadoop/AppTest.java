package HadoopDemo.HelloHadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.Test;
//import static junit.framework.Assert.*;
import java.net.URI;

public class AppTest {
	@Test
	public void testDemo() {
		System.out.println("Hello AppTest!");
	}
	
	//@Test
	public void testmkdir() throws Exception {
		System.out.println("Hello testmkdir!");
		
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop:9000"), configuration, "root");
		fs.mkdirs(new Path("/test0"));
		fs.close();
	}
}
