package viettel.Spark.streaming;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFS {
	private Configuration conf;
	private FileSystem fs; 
	private Path heartRateFile;
	private FSDataOutputStream out;
	final private int bufferSize = 512;
	
	/**
	 * Constructor for hdfs
	 */
	public HDFS() {
		String heartRatePath = "/home/kafka/heart_rate";
		// Path to file config
		String coreSite = "file:///home/hadoop/hadoop-3.1.2/etc/hadoop/core-site.xml";
		String hdfsSite = "file:///home/hadoop/hadoop-3.1.2/etc/hadoop/hdfs-site.xml";
		// Add config
		conf = new Configuration();
		conf.addResource(coreSite);
		conf.addResource(hdfsSite);
		
		try {
			
			fs = FileSystem.get(conf);
			heartRateFile = new Path(heartRatePath);
			// check exist and create file if it does not exist
			if(!fs.exists(heartRateFile)) {
				out = fs.create(heartRateFile);
			} else {
				out = fs.append(heartRateFile);
			}
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	/**
	 * 
	 * @param in
	 */
	public void writeToFile(InputStream in) {
		try {
			// Write from buffer to file
			byte[] buffer = new byte[bufferSize];
			int numRead;
			while((numRead = in.read(buffer)) > 0) {
				out.write(buffer);
			}
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			// Close input stream
			try {
				in.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} 
		
	}
	
	/**
	 * Close output stream to file
	 */
	public void close() {
		try {
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
