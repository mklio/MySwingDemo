package javatestdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import java.net.URI;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.matchers.JUnitMatchers.*; 

public class AppTest {
	@Before
	public void testRead() throws Exception {
		System.out.println("Hello testRead!");
	}
	
	@Test
	public void testmkdir() throws Exception {
		System.out.println("Hello testmkdir!");
	}
	
	@After
	public void testAfter() {
		//boolean b = assertThat("Hello testAfter!",is("Hello testAfter!"));
		System.out.println(is("Hello testAfter!"));
		assertThat("Hello testAfter!",is("Hello testAfter!"));
		//System.out.println(assertThat("Hello testAfter!",is("Hello testAfter!")));
	}
}
