package viettel.HDFS.write;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class DirectWriter {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		InputStream in = new FileInputStream(new File("/home/vttek/testFile.txt"));
		
		Configuration conf = new Configuration();
//		conf.set("fs.hdfs.impl", org.apache.hadoop.)
//		conf.set("fs.file.impl", org.apache.hadoop.fs.Loca) 
		
		Path coreSite = new Path("file:/home/hadoop/hadoop-3.1.2/etc/hadoop/core-site.xml");
		Path hdfsSite = new Path("file:/home/hadoop/hadoop-3.1.2/etc/hadoop/hdfs-site.xml");
		
		conf.addResource(coreSite);
		conf.addResource(hdfsSite);
		
		FileSystem fs = FileSystem.get(conf);
		
		Path outputFile = new Path("/home/hdfs/testFile.txt");
		OutputStream out;
		
		if(fs.exists(outputFile)) {
			out = fs.append(outputFile);
		} else {
			out = fs.create(outputFile);
		}
		
//		OutputStream out = new FileOutputStream(new File("hdfs://10.55.123.60:9000/home/hdfs/testFile.txt"));
		
		byte[] buffer = new byte[256];
		
		while(in.read(buffer) > 0) {
			out.write(buffer);
		}
		
		in.close();
		out.close();
	}

}
