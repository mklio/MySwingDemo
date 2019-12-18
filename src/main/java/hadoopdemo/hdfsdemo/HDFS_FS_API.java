package hadoopdemo.hdfsdemo;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class HDFS_FS_API {
	public static void main(String[] args) throws Exception {
		String uri = "hdfs://192.168.43.110:9000/test1/test01.txt";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
//		fs.mkdirs(new Path("hdfs://192.168.43.110:9000/test0"));
		
		InputStream in = null;
		try {
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 3, false);
			IOUtils.copyBytes(in, System.out, 3, false);
		} finally {
			IOUtils.closeStream(in);
		}
		
		FSDataInputStream in2 = null;
		try {
			in2 = fs.open(new Path(uri));
			IOUtils.copyBytes(in2, System.out, 1, false);
			in2.seek(0);
			IOUtils.copyBytes(in2, System.out, 2, false);
		} finally {
			IOUtils.closeStream(in2);
		}
		
		String local_uri = "D://TEST/test1.txt";
		InputStream in3 = new BufferedInputStream(new FileInputStream(local_uri));
		OutputStream out3 = fs.create(new Path(uri), new Progressable() {
			int i = 0;
			@Override
			public void progress() {
				// TODO Auto-generated method stub
				System.out.print(++i + "..");
			}
		});
		IOUtils.copyBytes(in3, out3, 1, true);
		
		OutputStream out4 = fs.create(new Path(uri));
		try {
			out4.write("hello world!".getBytes());
		} finally {
			out4.close();
		}
	}

}
