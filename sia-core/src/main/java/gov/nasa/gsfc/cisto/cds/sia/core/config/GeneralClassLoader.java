/**
 * 
 */
package gov.nasa.gsfc.cisto.cds.sia.core.config;

import gov.nasa.gsfc.cisto.cds.sia.core.common.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;

/**
 * Provides utilities for dynamically loading classes.
 *
 * @author Glenn Tamkin, NASA/CSC
 */
public class GeneralClassLoader {
	private static Logger logger = LoggerFactory.getLogger(GeneralClassLoader.class);

	/**
	 * Loads a groovy or java object based on the the suffix of the path.
	 *
	 * @param path     the path
	 * @param argTypes the arg types
	 * @param args     the args
	 * @return object
	 * @throws Exception the exception
	 */
	public static Object loadObject(String path, Class<?>[] argTypes, Object[] args) throws Exception {

		return loadJavaObject(path, argTypes, args);
	}

	/**
	 * Loads a java object from a special formatted "path" string. A path string
	 * is a colon separated string with the file path as the first part and the
	 * class name as the second part.
	 * <p>
	 * If the path is not colon separated, then it is treated as a classname.
	 *
	 * @param path     A colon separated filepath:classname pair, or just a class            name.
	 * @param argTypes The types of arguments for this class.
	 * @param args     The argument values.
	 * @return The java object, or null if it could not be created.
	 * @throws Exception the exception
	 */
	public static Object loadJavaObject(String path, Class<?>[] argTypes, Object[] args) throws Exception {
		if (path == null) {
			logger.error("No file:class specified. The class will not be loaded.");
			return null;
		}

		if (!path.contains(":")) {
			// If no : is present, then this is a class file reference.
			File executorFile = new File(path);

			if (executorFile.exists()) {
				return loadJavaObject(executorFile, null, argTypes, args);
			} else {
				// try just loading it as a class
				Object obj = loadJavaClass(path, argTypes, args);
				if (obj == null) {
					logger.error("The file or class " + path + " does not exist. The class could not be loaded.");
				}
				return obj;
			}
		} else {
			// if : is present, then this is a jar reference with a class name
			// specifier.
			String[] tokens = path.split(":");

			// Get the file name for this task. Files are always relative to the
			// working directory.
			File executorFile = new File(tokens[0]);

			if (executorFile.exists()) {
				return loadJavaObject(executorFile.getAbsolutePath(), tokens[1].trim(), argTypes, args);
			} else {
				logger.error("The file " + path + " does not exist. The class could not be loaded.");
				return null;
			}
		}
	}

	/**
	 * Helper method for loading java class objects. The class must be present
	 * in the current context in order for it to load.
	 *
	 * @param className The class name to laod.
	 * @param argTypes  The argument types.
	 * @param args      The arguments.
	 * @return The object, or null if it could not be loaded.
	 * @throws Exception the exception
	 */
	public static Object loadJavaClass(String className, Class<?>[] argTypes, Object[] args) throws Exception {
		// try loading it as a class
		try {
			Class<?> clazz = Class.forName(className, true, Thread.currentThread().getContextClassLoader());
			if (args != null) {
				Constructor<?> constructor = getCompatibleConstructor(clazz, argTypes);
				return constructor.newInstance(args);
			} else {
				Constructor<?> constructor = clazz.getDeclaredConstructor();
				return constructor.newInstance();
			}
		} catch (Exception ex) {
			logger.error("The class " + className + " could not be loaded.", ex);
			throw ex;
		}
	}

	/**
	 * Loads and creates a external class or object specified by the file and
	 * class name.
	 *
	 * @param fileName  the file name
	 * @param className The class to create an instance of.
	 * @param argTypes  The argument types to the object's constructor.
	 * @param args      The actual arguments.
	 * @return The new instance, or null if it could not be loaded.
	 * @throws Exception the exception
	 */
	public static Object loadJavaObject(String fileName, String className, Class<?>[] argTypes, Object[] args)
			throws Exception {
		if (fileName != null) {
			File file = new File(fileName);
			if (!file.exists()) {
				logger.error("Could not load class. The specified file " + fileName + " does not exist.");
				return null;
			} else {
				return loadJavaObject(file, className, argTypes, args);
			}
		}

		return null;
	}

	/**
	 * Loads and creates a external class or object specified by the file and
	 * class name.
	 *
	 * @param file      The file to load.
	 * @param className The class to create an instance of.
	 * @param argTypes  The argument types to the object's constructor.
	 * @param args      The actual arguments.
	 * @return The new instance, or null if it could not be loaded.
	 * @throws Exception the exception
	 */
	public static Object loadJavaObject(File file, String className, Class<?>[] argTypes, Object[] args)
			throws Exception {
		if (!file.exists()) {
			return null;
		}

		// create a class loader that loads jars from the directory (and
		// subdirectories of the file)
		URLClassLoader classLoader;
		if (file.isDirectory()) {
			classLoader = createDynamicLoader(file.getAbsolutePath());
		} else {
			File dir = file.getParentFile();
			classLoader = createDynamicLoader(dir.getAbsolutePath());
		}

		return loadJavaObject(classLoader, file, className, argTypes, args);
	}

	/**
	 * Loads and creates a external class or object specified by the file and
	 * class name. Uses a specified class loader.
	 *
	 * @param classLoader The class loader to use for .class and .jar files.
	 * @param file        The file to load.
	 * @param className   The class to create an instance of.
	 * @param argTypes    The argument types to the object's constructor.
	 * @param args        The actual arguments.
	 * @return The new instance, or null if it could not be loaded.
	 * @throws Exception the exception
	 */
	public static Object loadJavaObject(URLClassLoader classLoader, File file, String className, Class<?>[] argTypes,
										Object[] args) throws Exception {
		if (!file.exists()) {
			return null;
		}
		String name = file.getName();

		Object obj;

		if (name.endsWith(".jar") || name.endsWith(".class")) {
			obj = loadJavaObject(classLoader, file.getParent(), className, argTypes, args);
		} else {
			// just try loading it as a class
			obj = loadJavaClass(className, argTypes, args);
		}

		if (obj == null) {
			logger.error("Could not load java object specified by " + file.getAbsolutePath()
					+ ". File type must be a jar, class, groovy, or jython.");
		}
		return obj;
	}

	/**
	 * Helper method for loading java class objects
	 *
	 * @param classLoader the class loader
	 * @param path        The path of the class file.
	 * @param className   The class name to laod.
	 * @param argTypes    The argument types.
	 * @param args        The arguments.
	 * @return The object, or null if it could not be loaded.
	 * @throws Exception the exception
	 */
	public static Object loadJavaObject(URLClassLoader classLoader, String path, String className, Class<?>[] argTypes,
										Object[] args) throws Exception {
		try {
			Class<?> clazz = classLoader.loadClass(className);
			if (args != null) {
				Constructor<?> constructor = getCompatibleConstructor(clazz, argTypes);
				return constructor.newInstance(args);
			} else {
				Constructor<?> constructor = clazz.getDeclaredConstructor();
				return constructor.newInstance();
			}
		} catch (Exception ex) {
			logger.error("The class " + className + " could not be loaded from file " + path + ".", ex);
			throw ex;
		}
	}

	/**
	 * Get a compatible constructor for the given value type
	 *
	 * @param type       Class to look for constructor in
	 * @param valueTypes the value types
	 * @return Constructor or null
	 * @throws Exception the exception
	 */
	public static Constructor<?> getCompatibleConstructor(final Class<?> type, Class<?>[] valueTypes) throws Exception {
		// first try and find a constructor with the exact argument type
		try {
			return type.getConstructor(valueTypes);
		} catch (Exception ignore) {
			Constructor<?>[] constructors = type.getConstructors();
			for (Constructor<?> constructor : constructors) {
				Class<?>[] argTypes = constructor.getParameterTypes();

				// if they don't have the same number of arguments, skip it
				if (argTypes.length != valueTypes.length) {
					continue;
				}

				boolean found = true;
				for (int i = 0; i < argTypes.length; i++) {
					if (!argTypes[i].isAssignableFrom(valueTypes[i])) {
						// not a match, move on to the next constructor
						found = false;
						break;
					}
				}

				if (found) {
					return constructor;
				}
			}
		}

		throw new Exception("No constructor found.");
	}

	/**
	 * Creates a classloader based on the specified directory.
	 *
	 * @param directory The starting directory
	 * @return the url class loader
	 * @throws Exception the exception
	 */
	public static URLClassLoader createDynamicLoader(String directory) throws Exception {
		// Find all the jar files
		File file = new File(directory);

		// if the specified directory is actually a file, then just load the
		// file.
		if (!file.isDirectory() && (file.getName().endsWith(".jar") || file.getName().endsWith(".class"))) {
			// create the class loader based off the urls
			URL[] url = new URL[1];
			try {
				url[0] = file.toURI().toURL();
			} catch (MalformedURLException e) {
				if (logger != null) {
					logger.warn("There was a problem creating the dynamic class loader.", e);
				}

				throw e;
			}
			return URLClassLoader.newInstance(url, Thread.currentThread().getContextClassLoader());
		} else {
			// get all jar and class files.
			ArrayList<File> fileList = new ArrayList<File>();
			FileFilter classFilter = new FileFilter() {
				public boolean accept(File file) {
					if (file.isDirectory()) {
						return true;
					}

					return file.getName().endsWith(".jar") || file.getName().endsWith(".class");
				}
			};

			FileUtils.getFileList(file, fileList, classFilter, true);
			URL[] urlArray = new URL[fileList.size()];

			try {
				int i = 0;
				// now create urls to all the jar locations
				// first, create all the urls to scan
				for (File cdir : fileList) {
					URI uri = cdir.toURI();
					urlArray[i++] = uri.toURL();
					logger.debug("Class loader loading: " + uri.toString());
				}

				// create the class loader based off the urls
				return URLClassLoader.newInstance(urlArray, Thread.currentThread().getContextClassLoader());
			} catch (MalformedURLException ex) {
				if (logger != null) {
					logger.warn("There was a problem creating the dynamic class loader.", ex);
				}

				throw ex;
			}
		}
	}

}
