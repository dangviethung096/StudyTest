package viettel.HDFS.write;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class SimpleWriter {
	
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		String outputFile = "/home/hdfs/testFile.txt";
		String inputFile = "/home/vttek/testFile.txt";
		
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
