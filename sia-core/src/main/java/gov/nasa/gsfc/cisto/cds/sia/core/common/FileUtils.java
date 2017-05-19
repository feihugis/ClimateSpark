/**
 * 
 */
package gov.nasa.gsfc.cisto.cds.sia.core.common;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Properties;
import java.util.regex.Matcher;

/**
 * This class contains some useful file utilities.
 *
 * @author Glenn Tamkin, NASA/CSC
 */
public class FileUtils {

    /**
     * Recursive file list generator. A useful utility function.
     *
     * @param dir      The directory to search
     * @param fileList The arraylist to store found files
     * @param filter   The filter to apply to found files
     * @param recurse  True to recurse, false otherwise
     */
    public static void getFileList(File dir, ArrayList<File> fileList,
								   FileFilter filter, boolean recurse) {
		File[] files = dir.listFiles(filter);
		if (files != null) {
			if (recurse) {
				for (File file : files) {
					if (file.isDirectory()) {
						getFileList(file, fileList, filter, true);
					} else {
						fileList.add(file);
					}
				}
			} else {
				for (File file : files) {
					if (!file.isDirectory()) {
						fileList.add(file);
					}
				}
			}
		}
	}

    /**
     * Concatenates paths, ensuring that the correct path separator for this
     * system.
     *
     * @param paths The variable number of paths to concatenate.
     * @return The concatenated path.
     */
    public static String concatenatePaths(String... paths) {
		return concatenatePaths((char) 0, paths);
	}

    /**
     * Concatenates paths, using the specified path separator. This method
     * allows the user to concatenate paths for systems that may use a different
     * separator than the current system.
     *
     * @param pathSeparator The path separator to use.
     * @param paths         The variable number of paths to concatenate.
     * @return The concatenated path.
     */
    public static String concatenatePaths(char pathSeparator, String... paths) {
		String separator;
		if (pathSeparator == 0 || Character.isISOControl(pathSeparator)
				|| Character.isLetterOrDigit(pathSeparator)) {
			separator = File.separator;
		} else {
			separator = "" + pathSeparator;
		}

		// first go through and change all path separators to match the current
		// system
		ArrayList<String> pathSegs = new ArrayList<String>();
		if (separator.compareTo("/") == 0) {
			for (String path : paths) {
				if (path != null) {
					pathSegs.add(path.replaceAll(
							Matcher.quoteReplacement("\\"), separator).trim());
				}
			}
		} else {
			for (String path : paths) {
				if (path != null) {
					pathSegs.add(path.replaceAll("/",
							Matcher.quoteReplacement("\\")).trim());
				}
			}
		}

		StringBuilder builder = new StringBuilder();
		builder.append(pathSegs.get(0));

		for (int i = 1; i < pathSegs.size(); i++) {
			if (builder.lastIndexOf(separator) != builder.length() - 1) {
				if (!pathSegs.get(i).startsWith(separator)) {
					builder.append(separator);
					builder.append(pathSegs.get(i));
				} else {
					builder.append(pathSegs.get(i));
				}
			} else {
				if (!pathSegs.get(i).startsWith(separator)) {
					builder.append(pathSegs.get(i));
				} else {
					builder.append(pathSegs.get(i).substring(1));
				}
			}
		}
		return builder.toString();
	}

    /**
     * Returns a file system object based on the specified path.
     *
     * @param path The path (local or URI)
     * @return The file system.
     * @throws Exception the exception
     */
    public static FileSystem getFileSystem(String path) throws Exception {
		// get file system
		FileSystem fs;
		Configuration conf;
		if (path.startsWith("hdfs://")) {
			// store to hadoop
			conf = new Configuration();
			fs = FileSystem.get(URI.create(path), conf);
		} else {
			// store to local
			conf = new Configuration();
			fs = FileSystem.getLocal(conf);
		}

		return fs;
	}

    /**
     * Gets a list of files for the specified path.
     *
     * @param ifs       the ifs
     * @param path      The path.
     * @param filter    An array of suffixes to search for.
     * @param fileList  the file list
     * @param recursive Whether or not to recurse into sub directories.
     * @return The list of files.
     * @throws Exception the exception
     */
    public static void getFileList(FileSystem ifs, Path path,
								   PathFilter filter, ArrayList<FileStatus> fileList, boolean recursive)
			throws Exception {
		FileStatus[] list = ifs.listStatus(path, filter);
		if (recursive) {
			for (FileStatus file : list) {
				if (file.isDir()) {
					getFileList(ifs, file.getPath(), filter, fileList,
							recursive);
				} else {
					fileList.add(file);
				}
			}
		} else {
			for (FileStatus file : list) {
				if (!file.isDir()) {
					fileList.add(file);
				}
			}
		}
	}

    /**
     * Configure properties.
     *
     * @param filename the filename
     * @param logger   the logger
     */
    public static void configureProperties(String filename, Logger logger) {
		File propertiesFile = new File(filename);
		if (propertiesFile.exists()) {
			PropertyConfigurator.configure(propertiesFile.toString());
		} else {
			logger.info("Unable to read or parse property file [" + filename
					+ "].  Using default properties.");
			Properties props = new Properties();
			PropertyConfigurator.configure(props);
		}
	}

    /**
     * Gets properties.
     *
     * @param filename the filename
     * @param log      the log
     * @return the properties
     */
    public static Properties getProperties(String filename, Log log) {
		Properties properties = new Properties();
		FileReader reader = null;
		try {
			reader = new FileReader(filename);
			properties.load(reader);
		} catch (Exception ex) {
			if (log != null) {
				log.warn("Unable to access file <" + filename + ">.");
			}
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					if (log != null) {
						log.info("Unable to close file <" + filename + ">.");
					}
				}
			}
		}
		return properties;
	}

    /**
     * String array to int array int [ ].
     *
     * @param inputs the inputs
     * @return the int [ ]
     */
    public static int[] stringArrayToIntArray(String[] inputs) {
          int[] result = new int[inputs.length];
          for (int i=0; i<result.length; i++) {
            result[i] = Integer.parseInt(inputs[i]);
          }
          return result;
        }

}
