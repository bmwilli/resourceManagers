/**
 * 
 */
package com.ibm.streams.resourcemgr.mesos;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * General helper functions for Posix/Linux file system support
 * In separate file to avoid confusion of java.nio.files.Path and hadoop.Path
 * @author Brian M Williams
 *
 */
public class LocalFSUtils {
	public static String copyToLocal(String fromFile, String toDir) throws IOException {
		Path source = Paths.get(fromFile);
		Path destPath = Paths.get(toDir, source.getFileName().toString());
		Files.copy(source, destPath,StandardCopyOption.REPLACE_EXISTING);
		
		return destPath.toString();
	}
}
