package hadoopdemo.hdfsdemo;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class ShowList {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String uri = "hdfs://192.168.43.110:9000/test1/test01.txt";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path[] paths = new Path[2];
		paths[0] = new Path("/");
		paths[1] = new Path("/test1");
		
		FileStatus[] status = fs.listStatus(paths);
		Path[] listPaths = FileUtil.stat2Paths(status);

		for(Path p : listPaths) {
			System.out.println(p);
		}
		
		for(FileStatus statu : status) {
			System.out.println(statu.getOwner());
		}
	}

}
