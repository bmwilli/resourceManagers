/**
 * 
 */
package com.ibm.streams.resourcemgr.mesos;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * General helper files for HDFS file system, copied from ibm stream yarn impl
 * 
 * @author Brian M Williams
 *
 */
public class HdfsFSUtils {
	private static final Logger LOG = LoggerFactory.getLogger(HdfsFSUtils.class);
	
	/** Returns the path on the HDFS for a particular application
	 * @param applicationID
	 * @param name
	 * @return HDFS path for application
	 */
	public static String getHDFSPath(String applicationID, String name) {
		return applicationID + Path.SEPARATOR + name;
	}
	
	public static Path copyToHDFS(FileSystem hdfs, String hdfsPathPrefix, String localPath, String name) throws IOException {
		Path hdfsPath = new Path(hdfs.getHomeDirectory(), getHDFSPath(hdfsPathPrefix, name));
		LOG.info("copying local: " + localPath + " to hdfs: " + hdfsPath);
		hdfs.copyFromLocalFile(new Path(localPath),  hdfsPath);
		return hdfsPath;
	}
	
	/* Need to add arguments to supply or override constants */
	public static FileSystem getHDFSFileSystem() throws IOException {
		Configuration conf = new Configuration();
		LOG.debug("HDFS conf: " + conf.toString());
		LOG.debug("*** fs.default.name = " + conf.getRaw("fs.default.name"));
		
		FileSystem fs = FileSystem.get(conf);
		
		LOG.info("fs.getHomeDirectory();: " + fs.getHomeDirectory());
		
		return fs;
	}
}
