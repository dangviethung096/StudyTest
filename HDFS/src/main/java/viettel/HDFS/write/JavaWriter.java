package viettel.HDFS.write;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class JavaWriter {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		Path coreSite = new Path("file:///home/hadoop/hadoop-3.1.2/etc/hadoop/core-site.xml");
		Path hdfsSite = new Path("file:///home/hadoop/hadoop-3.1.2/etc/hadoop/hdfs-site.xml");
		conf.addResource(coreSite);
		conf.addResource(hdfsSite);
		
		
		String outputFile = "hdfs://10.55.123.60:9000/home/hdfs/testFile.txt";
		String inputFile = "file:///home/vttek/testFile.txt";
		
		InputStream in = null;
		FSDataOutputStream out = null;
		try {
			FileSystem fs = FileSystem.get(conf); 
			// Hadoop DFS Path
//			Path inFile = new Path(args[0]);
			Path outFile = new Path(outputFile);
			// check exist
			if(!(new File(inputFile).exists()) ) {
				System.out.println("Input file does not exist");
				throw new IOException("Input file does not exist");
			}
			
			if(fs.exists(outFile)) {
				System.out.println("Output file already exists");
				throw new IOException("Output file already exists");
			} 
			
			try {
				in = new BufferedInputStream(new FileInputStream(inputFile));
				
				out = fs.create(outFile);
				
				IOUtils.copyBytes(in, out, 512, false);
				
			} finally {
				IOUtils.closeStream(in);
				IOUtils.closeStream(out);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
