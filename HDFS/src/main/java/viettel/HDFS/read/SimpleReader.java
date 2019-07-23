package viettel.HDFS.read;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class SimpleReader {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in = null;
			
			Path inFile = new Path(args[0]);
			if(!fs.exists(inFile)) {
				System.out.println("Input file not found");
				return;
			}
			
			try {
				in = fs.open(inFile);
				
				IOUtils.copyBytes(in, System.out, 512, false);
			} finally {
				IOUtils.closeStream(in);
			}
		} catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
