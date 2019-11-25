package HadoopDemo.HelloHadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
//import static junit.framework.Assert.*;
import java.net.URI;

public class AppTest {
	@Before
	public void testBefore() {
		System.out.println("Hello testBefore!");
	}
	
	@Test
	public void testmkdir() throws Exception {
		System.out.println("Hello testmkdir!");
		
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop:9000"), configuration, "root");
		//fs.mkdirs(new Path("/test1"));
		fs.close();
	}
	
	@After
	public void testAfter() {
		System.out.println("Hello testAfter!");
	}
}
