package gov.nasa.gsfc.cisto.cds.sia.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The type Utils.
 */
public class Utils {

    /**
     * The constant VARIABLE_NAME.
     */
    public static final String VARIABLE_NAME = "damasc.variable_name";
    /**
     * The constant SAMPLE_RATIO.
     */
    public static final String SAMPLE_RATIO = "damasc.sample_ratio";
    /**
     * The constant VARIABLE_SHAPE_PREFIX.
     */
    public static final String VARIABLE_SHAPE_PREFIX = "damasc.variable_shapes";
    /**
     * The constant LOW_FILTER.
     */
    public static final String LOW_FILTER = "damasc.low_filter";
    /**
     * The constant HIGH_FILTER.
     */
    public static final String HIGH_FILTER = "damasc.high_filter";
    /**
     * The constant PART_MODE.
     */
    public static final String PART_MODE = "damasc.partition_mode";
    /**
     * The constant PLACEMENT_MODE.
     */
    public static final String PLACEMENT_MODE = "damasc.placement_mode";
    /**
     * The constant NO_SCAN.
     */
    public static final String NO_SCAN = "damasc.noscan";
    /**
     * The constant USE_COMBINER.
     */
    public static final String USE_COMBINER = "damasc.use_combiner";
    /**
     * The constant HOLISTIC.
     */
    public static final String HOLISTIC = "damasc.holistic";
    /**
     * The constant QUERY_DEPENDANT.
     */
    public static final String QUERY_DEPENDANT = "damasc.query_dependant";
    /**
     * The constant EXTRACTION_SHAPE.
     */
    public static final String EXTRACTION_SHAPE = "damasc.extraction_shape";
    /**
     * The constant NUMBER_REDUCERS.
     */
    public static final String NUMBER_REDUCERS = "damasc.number_reducers";
    /**
     * The constant OPERATOR.
     */
    public static final String OPERATOR = "damasc.operator";
    /**
     * The constant DEBUG_LOG_FILE.
     */
    public static final String DEBUG_LOG_FILE = "damasc.logfile";
    /**
     * The constant BUFFER_SIZE.
     */
    public static final String BUFFER_SIZE = "damasc.buffer_size";
    /**
     * The constant MULTI_FILE_MODE.
     */
    public static final String MULTI_FILE_MODE = "damasc.multi_file_mode";
    /**
     * The constant FILE_NAME_ARRAY.
     */
    public static final String FILE_NAME_ARRAY = "damasc.file_name_array";
    /**
     * The constant DEFAULT_BUFFER_SIZE.
     */
    public static final String DEFAULT_BUFFER_SIZE = "8192";
	private static final Log LOG = LogFactory.getLog(Utils.class);
	private static float sampleRatio;
	private static boolean sampleRatioSet = false;
	private static int[] variableShape;
	private static boolean variableShapeSet = false;
	private static int validLow;
	private static int validHigh;
	private static boolean validSet = false;
	private static Operator operator = Operator.optUnknown;
	private static PartMode partMode = PartMode.proportional;
	private static PlacementMode placementMode = PlacementMode.roundrobin;
	private static boolean operatorSet = false;
	private static boolean partModeSet = false;
	private static boolean placementModeSet = false;
	private static int[] extractionShape;
	private static boolean extractionShapeSet = false;
	private static boolean holisticEnabled = false;
	private static boolean holisticEnabledSet = false;
	private static boolean queryDependantEnabled = false;
	private static boolean queryDependantEnabledSet = false;
	private static boolean noScanEnabled = false;
	private static boolean noScanEnabledSet = false;
	private static boolean useCombiner = false;
	//GT - turned off combiner
	private static boolean useCombinerSet = false;
	//	private static boolean useCombinerSet = true;
	private static int numberReducers = 200;
	private static boolean numberReducersSet = false;
	private static String debugLogFile = "";
	private static boolean debugLogFileSet = false;
	private static int bufferSize = -1;
	private static boolean bufferSizeSet = false;
	private static MultiFileMode multiFileMode = MultiFileMode.combine;
	private static boolean multiFileModeSet = false;

    /**
     * Add file name int.
     *
     * @param fileName the file name
     * @param conf     the conf
     * @return the int
     */
    public static int addFileName(String fileName, Configuration conf) {
		ArrayList<String> fileNameArray = new ArrayList<String>(conf.getStringCollection("damasc.file_name_array"));
		fileNameArray.add(fileName);
		int retVal = fileNameArray.indexOf(fileName);

		conf.setStrings("damasc.file_name_array", stringArrayToString(fileNameArray));

		return retVal;
	}

    /**
     * Parse boundary box hash map.
     *
     * @param inputBoundaryBox the input boundary box
     * @return the hash map
     */
    public static HashMap<String, List<Integer[]>> parseBoundaryBox(String inputBoundaryBox) {
		HashMap<String, List<Integer[]>> boundaryBoxMap = new HashMap<String, List<Integer[]>>();
		List<Integer[]> startList = new ArrayList<Integer[]>();
		List<Integer[]> endList = new ArrayList<Integer[]>();
		List<String> boundaryBoxStringList = new ArrayList<String>();

		//interpret the input bounding box - regular expression
		//bounding box is input as, for example, [0-1,0-360,0-540]
		String template = "(\\d+-\\d+,){1,}\\d+-\\d+";
		Pattern pattern = Pattern.compile(template);
		Matcher matcher = pattern.matcher(inputBoundaryBox);
		String result;
		//for inputting several bounding box
		if (matcher.find()) {
			int i = 1;
			while (matcher.find(i)) {
				result = matcher.group(0);
				boundaryBoxStringList.add(result);

				//Check to see if there was more than one bounding box input by user
				i = i + result.length() + 3;
				i = i > inputBoundaryBox.length() ? inputBoundaryBox.length() : i;
			}
		}

		template = "\\d+";

		//list of bounding boxes separated by comma - ex. 0-1,0-361,0-540 , 0-41,0-100,0-200; each value is a single
		//bounding box, so for the first pass through it would handle the bbox 0-1,0-361,0-540 in this example
		for (String value : boundaryBoxStringList) {
			int numDimensions = value.split(",").length;

			// Ultimately, a list of the start corners for 0-1,0-361,0-540 would be
			// 0,0,0
			// A list of the end corners would be
			// 1,361,540
			Integer[] startCorner = new Integer[numDimensions];
			Integer[] endCorner = new Integer[numDimensions];
			pattern = Pattern.compile(template);
			matcher = pattern.matcher(value);
			if (matcher.find()) {
				int i = 0;
				int j = 0;
				while (matcher.find(i)) {
					result = matcher.group(0);

					if (j % 2 == 0) {
						startCorner[(j / 2)] = Integer.parseInt(result);
					} else {
						endCorner[((j - 1) / 2)] = Integer.parseInt(result);
					}
					i = i + result.length() + 1;
					i = i > value.length() ? value.length() : i;
					j++;
				}
			}
			startList.add(startCorner);
			endList.add(endCorner);
		}

		boundaryBoxMap.put("start", startList);
		boundaryBoxMap.put("end", endList);
		return boundaryBoxMap;
	}

    /**
     * String array to string string.
     *
     * @param strings the strings
     * @return the string
     */
    public static String stringArrayToString(Collection<String> strings) {
		String retString = "";
		int numElements = 0;
		for (String s : strings) {
			if (numElements > 0) {
				retString = retString + "," + s;
			} else {
				retString = s;
			}
			numElements++;
		}

		return retString;
	}

    /**
     * Sets variable shape.
     *
     * @param conf          the conf
     * @param variableShape the variable shape
     */
    public static void setVariableShape(Configuration conf, String variableShape) {
		conf.set("damasc.variable_shapes", variableShape);
		variableShapeSet = true;
	}

    /**
     * Get variable shape int [ ].
     *
     * @param conf the conf
     * @return the int [ ]
     */
    public static int[] getVariableShape(Configuration conf) {
		if (!variableShapeSet) {
			String dimString = conf.get("damasc.variable_shapes", "");

			String[] dimStrings = dimString.split(",");
			variableShape = new int[dimStrings.length];

			for (int i = 0; i < variableShape.length; i++) {
				variableShape[i] = Integer.parseInt(dimStrings[i]);
			}

			variableShapeSet = true;
		}

		return variableShape;
	}

    /**
     * Variable shape set boolean.
     *
     * @return the boolean
     */
    public static boolean variableShapeSet() {
		return variableShapeSet;
	}

    /**
     * Gets variable name.
     *
     * @param conf the conf
     * @return the variable name
     */
    public static String getVariableName(Configuration conf) {

		return conf.get("damasc.variable_name", "");
	}

    /**
     * Gets debug log file name.
     *
     * @param conf the conf
     * @return the debug log file name
     */
    public static String getDebugLogFileName(Configuration conf) {
		if (!debugLogFileSet) {
			debugLogFile = conf.get("damasc.logfile", "");
			debugLogFileSet = true;
		}

		return debugLogFile;
	}

    /**
     * Gets buffer size.
     *
     * @param conf the conf
     * @return the buffer size
     */
    public static int getBufferSize(Configuration conf) {
		if (!bufferSizeSet) {
			bufferSize = Integer.parseInt(conf.get("damasc.buffer_size", "8192"));
			bufferSizeSet = true;
		}

		return bufferSize;
	}

    /**
     * Gets multi file mode.
     *
     * @param conf the conf
     * @return the multi file mode
     */
    public static MultiFileMode getMultiFileMode(Configuration conf) {
		if (!multiFileModeSet) {
			String multiFileModeString = getMultiFileModeString(conf);
			multiFileMode = parseMultiFileMode(multiFileModeString);
			multiFileModeSet = true;
		}

		return multiFileMode;
	}

    /**
     * Gets sample ratio.
     *
     * @param conf the conf
     * @return the sample ratio
     */
    public static float getSampleRatio(Configuration conf) {
		if (!sampleRatioSet) {
			sampleRatio = conf.getFloat("damasc.sample_ratio", 0.01F);
			sampleRatioSet = true;
		}

		return sampleRatio;
	}

    /**
     * Gets valid low.
     *
     * @param conf the conf
     * @return the valid low
     */
    public static int getValidLow(Configuration conf) {
		if (!validSet) {
			validLow = conf.getInt("damasc.low_filter", Integer.MIN_VALUE);
			validHigh = conf.getInt("damasc.high_filter", Integer.MAX_VALUE);
			validSet = true;
		}

		return validLow;
	}

    /**
     * Gets valid high.
     *
     * @param conf the conf
     * @return the valid high
     */
    public static int getValidHigh(Configuration conf) {
		if (!validSet) {
			validLow = conf.getInt("damasc.low_filter", Integer.MIN_VALUE);
			validHigh = conf.getInt("damasc.high_filter", Integer.MAX_VALUE);
			validSet = true;
		}

		return validHigh;
	}

    /**
     * Gets no scan string.
     *
     * @param conf the conf
     * @return the no scan string
     */
    public static String getNoScanString(Configuration conf) {
		return conf.get("damasc.noscan", "TRUE");
	}

    /**
     * Gets number reducers string.
     *
     * @param conf the conf
     * @return the number reducers string
     */
    public static String getNumberReducersString(Configuration conf) {
		return conf.get("damasc.number_reducers", "1");
	}

    /**
     * Gets use combiner string.
     *
     * @param conf the conf
     * @return the use combiner string
     */
    public static String getUseCombinerString(Configuration conf) {
		return conf.get("damasc.use_combiner", "TRUE");
	}

    /**
     * Gets query dependant string.
     *
     * @param conf the conf
     * @return the query dependant string
     */
    public static String getQueryDependantString(Configuration conf) {
		return conf.get("damasc.query_dependant", "FALSE");
	}

    /**
     * Gets holistic string.
     *
     * @param conf the conf
     * @return the holistic string
     */
    public static String getHolisticString(Configuration conf) {
		return conf.get("damasc.holistic", "FALSE");
	}

    /**
     * Gets operator string.
     *
     * @param conf the conf
     * @return the operator string
     */
    public static String getOperatorString(Configuration conf) {
		return conf.get("damasc.operator", "");
	}

    /**
     * Gets part mode string.
     *
     * @param conf the conf
     * @return the part mode string
     */
    public static String getPartModeString(Configuration conf) {
		return conf.get("damasc.partition_mode", "Record");
	}

    /**
     * Gets placement mode string.
     *
     * @param conf the conf
     * @return the placement mode string
     */
    public static String getPlacementModeString(Configuration conf) {
		return conf.get("damasc.placement_mode", "Sampling");
	}

    /**
     * Gets multi file mode string.
     *
     * @param conf the conf
     * @return the multi file mode string
     */
    public static String getMultiFileModeString(Configuration conf) {
		return conf.get("damasc.multi_file_mode", "concat");
	}

    /**
     * Parse number reducers string int.
     *
     * @param numberReducersString the number reducers string
     * @return the int
     */
    public static int parseNumberReducersString(String numberReducersString) {
		int retVal = 1;
		try {
			retVal = Integer.parseInt(numberReducersString);
		} catch (NumberFormatException nfe) {
			LOG.info("nfe caught in parseNumberReducersString on string " + numberReducersString
					+ ". Using 1 as a default");
			retVal = 1;
		}

		return retVal;
	}

    /**
     * Parse use combiner string boolean.
     *
     * @param useCombinerString the use combiner string
     * @return the boolean
     */
    public static boolean parseUseCombinerString(String useCombinerString) {
		return useCombinerString.compareToIgnoreCase("True") == 0;
	}

    /**
     * Parse no scan string boolean.
     *
     * @param noScanString the no scan string
     * @return the boolean
     */
    public static boolean parseNoScanString(String noScanString) {
		return noScanString.compareToIgnoreCase("True") == 0;
	}

    /**
     * Parse query dependant string boolean.
     *
     * @param queryDependantString the query dependant string
     * @return the boolean
     */
    public static boolean parseQueryDependantString(String queryDependantString) {
		return queryDependantString.compareToIgnoreCase("True") == 0;
	}

    /**
     * Parse holistic string boolean.
     *
     * @param holisticString the holistic string
     * @return the boolean
     */
    public static boolean parseHolisticString(String holisticString) {
		return holisticString.compareToIgnoreCase("True") == 0;
	}

    /**
     * Parse multi file mode multi file mode.
     *
     * @param multiFileModeString the multi file mode string
     * @return the multi file mode
     */
    public static MultiFileMode parseMultiFileMode(String multiFileModeString) {
		MultiFileMode multiFileMode = MultiFileMode.combine;

		if (multiFileModeString.compareToIgnoreCase("Combine") == 0) {
			multiFileMode = MultiFileMode.combine;
		} else if (multiFileModeString.compareToIgnoreCase("Concat") == 0) {
			multiFileMode = MultiFileMode.concat;
		}

		return multiFileMode;
	}

    /**
     * Parse placement mode placement mode.
     *
     * @param placementModeString the placement mode string
     * @return the placement mode
     */
    public static PlacementMode parsePlacementMode(String placementModeString) {
		PlacementMode placementMode = PlacementMode.roundrobin;

		if (placementModeString.compareToIgnoreCase("RoundRobin") == 0) {
			placementMode = PlacementMode.roundrobin;
		} else if (placementModeString.compareToIgnoreCase("Sampling") == 0) {
			placementMode = PlacementMode.sampling;
		} else if (placementModeString.compareToIgnoreCase("Implicit") == 0) {
			placementMode = PlacementMode.implicit;
		}

		return placementMode;
	}

    /**
     * Parse part mode part mode.
     *
     * @param partModeString the part mode string
     * @return the part mode
     */
    public static PartMode parsePartMode(String partModeString) {
		PartMode partMode = PartMode.proportional;

		if (partModeString.compareToIgnoreCase("Proportional") == 0) {
			partMode = PartMode.proportional;
		} else if (partModeString.compareToIgnoreCase("Record") == 0) {
			partMode = PartMode.record;
		} else if (partModeString.compareToIgnoreCase("Calculated") == 0) {
			partMode = PartMode.calculated;
		} else {
			LOG.warn("Specified partition mode is not understood: " + partModeString + "\n"
					+ "Please specify one of the following: proportional, record, calculated");
		}

		return partMode;
	}

    /**
     * Parse operator operator.
     *
     * @param operatorString the operator string
     * @return the operator
     */
    public static Operator parseOperator(String operatorString) {
		Operator op;

		if (operatorString.compareToIgnoreCase("Max") == 0) {
			op = Operator.max;
		} else {
			if (operatorString.compareToIgnoreCase("SimpleMax") == 0) {
				op = Operator.simpleMax;
			} else {
				if (operatorString.compareToIgnoreCase("Median") == 0) {
					op = Operator.median;
				} else {
					if (operatorString.compareToIgnoreCase("SimpleMedian") == 0) {
						op = Operator.simpleMedian;
					} else {
						if (operatorString.compareToIgnoreCase("NullTest") == 0) {
							op = Operator.nulltest;
						} else {
							if (operatorString.compareToIgnoreCase("Average") == 0) {
								op = Operator.average;
							} else {
								op = Operator.optUnknown;
							}
						}
					}
				}
			}
		}
		return op;
	}

    /**
     * Gets part mode.
     *
     * @param conf the conf
     * @return the part mode
     */
    public static PartMode getPartMode(Configuration conf) {
		if (!partModeSet) {
			String partModeString = getPartModeString(conf);
			partMode = parsePartMode(partModeString);
			partModeSet = true;
		}

		return partMode;
	}

    /**
     * Gets placement mode.
     *
     * @param conf the conf
     * @return the placement mode
     */
    public static PlacementMode getPlacementMode(Configuration conf) {
		if (!placementModeSet) {
			String placementModeString = getPlacementModeString(conf);
			placementMode = parsePlacementMode(placementModeString);
			placementModeSet = true;
		}

		return placementMode;
	}

    /**
     * Use combiner boolean.
     *
     * @param conf the conf
     * @return the boolean
     */
    public static boolean useCombiner(Configuration conf) {
		if (!useCombinerSet) {
			String useCombinerString = getUseCombinerString(conf);
			useCombiner = parseUseCombinerString(useCombinerString);
			useCombinerSet = true;
		}

		return useCombiner;
	}

    /**
     * Gets number reducers.
     *
     * @param conf the conf
     * @return the number reducers
     */
    public static int getNumberReducers(Configuration conf) {
		if (!numberReducersSet) {
			String numberReducersString = getNumberReducersString(conf);
			numberReducers = parseNumberReducersString(numberReducersString);
			numberReducersSet = true;
		}

		return numberReducers;
	}

    /**
     * No scan enabled boolean.
     *
     * @param conf the conf
     * @return the boolean
     */
    public static boolean noScanEnabled(Configuration conf) {
		if (!noScanEnabledSet) {
			String noScanString = getNoScanString(conf);
			noScanEnabled = parseNoScanString(noScanString);
			noScanEnabledSet = true;
		}
		return noScanEnabled;
	}

    /**
     * Query dependant enabled boolean.
     *
     * @param conf the conf
     * @return the boolean
     */
    public static boolean queryDependantEnabled(Configuration conf) {
		if (!queryDependantEnabledSet) {
			String queryDependantString = getQueryDependantString(conf);
			queryDependantEnabled = parseQueryDependantString(queryDependantString);
			queryDependantEnabledSet = true;
		}
		return queryDependantEnabled;
	}

    /**
     * Holistic enabled boolean.
     *
     * @param conf the conf
     * @return the boolean
     */
    public static boolean holisticEnabled(Configuration conf) {
		if (!holisticEnabledSet) {
			String holisticString = getHolisticString(conf);

			holisticEnabled = parseHolisticString(holisticString);

			holisticEnabledSet = true;
		}

		return holisticEnabled;
	}

    /**
     * Gets operator.
     *
     * @param conf the conf
     * @return the operator
     */
    public static Operator getOperator(Configuration conf) {
		if (!operatorSet) {
			String operatorString = getOperatorString(conf);

			operator = parseOperator(operatorString);

			operatorSet = true;
		}

		return operator;
	}

    /**
     * Get extraction shape int [ ].
     *
     * @param conf the conf
     * @param size the size
     * @return the int [ ]
     */
    public static int[] getExtractionShape(Configuration conf, int size) {
		if (!extractionShapeSet) {
			String extractionString = conf.get("damasc.extraction_shape", "");

			if (extractionString.equals("")) {
				extractionShape = new int[size];

				for (int i = 0; i < size; i++) {
					extractionShape[i] = 1;
				}
			} else {
				String[] extractionDims = extractionString.split(",");

				extractionShape = new int[extractionDims.length];

				for (int i = 0; i < extractionShape.length; i++) {
					extractionShape[i] = Integer.parseInt(extractionDims[i]);
				}
			}

			extractionShapeSet = true;
		}

		return extractionShape;
	}

    /**
     * Is valid boolean.
     *
     * @param globalCoord the global coord
     * @param conf        the conf
     * @return the boolean
     */
    public static boolean isValid(int[] globalCoord, Configuration conf) {
		if (!validSet) {
			getValidLow(conf);
		}

		return (globalCoord[0] >= validLow) && (globalCoord[0] < validHigh);
	}

    /**
     * Array to string string.
     *
     * @param array the array
     * @return the string
     */
    public static String arrayToString(int[] array) {
		String tempStr = "";

		if (array == null) {
			return tempStr;
		}

		for (int i = 0; i < array.length; i++) {
			if (i > 0) {
				tempStr = tempStr + ",";
			}
			tempStr = tempStr + array[i];
		}

		return tempStr;
	}

    /**
     * Array to string string.
     *
     * @param array the array
     * @return the string
     */
    public static String arrayToString(long[] array) {
		String tempStr = "";

		for (int i = 0; i < array.length; i++) {
			if (i > 0) {
				tempStr = tempStr + ",";
			}
			tempStr = tempStr + array[i];
		}

		return tempStr;
	}

    /**
     * End of variable boolean.
     *
     * @param varShape the var shape
     * @param current  the current
     * @return the boolean
     */
    public static boolean endOfVariable(int[] varShape, int[] current) {
		boolean retVal = true;

		if (current[0] >= varShape[0]) {
			return retVal;
		}

		for (int i = 0; i < current.length; i++) {
			if (current[i] < varShape[i] - 1) {
				retVal = false;
				return retVal;
			}
		}

		return retVal;
	}

    /**
     * At full record boolean.
     *
     * @param corner the corner
     * @return the boolean
     */
    public static boolean atFullRecord(int[] corner) {
		boolean retVal = true;

		for (int i = 1; i < corner.length; i++) {
			if (corner[i] != 0) {
				retVal = false;
			}
		}

		return retVal;
	}

    /**
     * Increment array int [ ].
     *
     * @param varShape the var shape
     * @param current  the current
     * @return the int [ ]
     */
    public static int[] incrementArray(int[] varShape, int[] current) {
		int curDim = current.length - 1;
		current[curDim] += 1;

		while ((current[curDim] >= varShape[curDim]) && (curDim > 0)) {
			current[curDim] = 0;
			current[(curDim - 1)] += 1;
			curDim--;
		}

		return current;
	}

    /**
     * Calc total size int.
     *
     * @param array the array
     * @return the int
     */
    public static int calcTotalSize(int[] array) {
		int retVal = 1;

		for (int anArray : array) {
			retVal *= anArray;
		}

		return retVal;
	}

    /**
     * Calc array total size long.
     *
     * @param array        the array
     * @param dataTypeSize the data type size
     * @return the long
     */
    public static long calcArrayTotalSize(int[] array, int dataTypeSize) {
		long retVal = 1L;

		for (int anArray : array) {
			retVal *= anArray;
		}

		retVal *= dataTypeSize;

		return retVal;
	}

    /**
     * Compute strides long [ ].
     *
     * @param shape the shape
     * @return the long [ ]
     * @throws IOException the io exception
     */
    public static long[] computeStrides(int[] shape) throws IOException {
		long[] stride = new long[shape.length];
		long product = 1L;
		for (int i = shape.length - 1; i >= 0; i--) {
			int dim = shape[i];
			if (dim < 0) {
				throw new IOException("Negative array size");
			}
			stride[i] = product;
			product *= dim;
		}
		return stride;
	}

    /**
     * Inflate int [ ].
     *
     * @param variableShape the variable shape
     * @param element       the element
     * @return the int [ ]
     * @throws IOException the io exception
     */
    public static int[] inflate(int[] variableShape, long element) throws IOException {
		int[] retArray = new int[variableShape.length];

		long[] strides = computeStrides(variableShape);

		for (int i = 0; i < variableShape.length; i++) {
			retArray[i] = ((int) (element / strides[i]));
			element -= retArray[i] * strides[i];
		}

		return retArray;
	}

    /**
     * Flatten long.
     *
     * @param variableShape  the variable shape
     * @param currentElement the current element
     * @return the long
     * @throws IOException the io exception
     */
    public static long flatten(int[] variableShape, int[] currentElement) throws IOException {
		return calcLinearElementNumber(variableShape, currentElement);
	}

    /**
     * Calc linear element number long.
     *
     * @param variableShape  the variable shape
     * @param currentElement the current element
     * @return the long
     * @throws IOException the io exception
     */
    public static long calcLinearElementNumber(int[] variableShape, int[] currentElement) throws IOException {
		long[] strides = computeStrides(variableShape);

		long retVal = 0L;

		for (int i = 0; i < currentElement.length; i++) {
			retVal += strides[i] * currentElement[i];
		}

		return retVal;
	}

    /**
     * Is sorted boolean.
     *
     * @param array the array
     * @return the boolean
     */
    public static boolean isSorted(int[] array) {
		if (array.length <= 1) {
			return true;
		}

		for (int i = 0; i < array.length - 1; i++) {
			if (array[i] > array[(i + 1)]) {
				return false;
			}
		}

		return true;
	}

    /**
     * Is sorted boolean.
     *
     * @param array the array
     * @return the boolean
     */
    public static boolean isSorted(long[] array) {
		if (array.length <= 1) {
			return true;
		}

		for (int i = 0; i < array.length - 1; i++) {
			if (array[i] > array[(i + 1)]) {
				return false;
			}
		}

		return true;
	}

    /**
     * Offset to block block location.
     *
     * @param blocks the blocks
     * @param offset the offset
     * @return the block location
     */
    public static BlockLocation offsetToBlock(BlockLocation[] blocks, long offset) {
		int j = blocks.length;
		for (int i = 0; i < j; i++) {
			BlockLocation block = blocks[i];
			long start = block.getOffset();
			long end = start + block.getLength();
			if ((start <= offset) && (offset < end)) {
				return block;
			}
		}
		return null;
	}

    /**
     * Map to global int [ ].
     *
     * @param currentCounter   the current counter
     * @param corner           the corner
     * @param globalCoordinate the global coordinate
     * @return the int [ ]
     */
    public static int[] mapToGlobal(int[] currentCounter, int[] corner, int[] globalCoordinate) {
		for (int i = 0; i < currentCounter.length; i++) {
			currentCounter[i] += corner[i];
		}
		return globalCoordinate;
	}

    /**
     * Is same hosts boolean.
     *
     * @param hosts1 the hosts 1
     * @param hosts2 the hosts 2
     * @return the boolean
     */
    public static boolean isSameHosts(String[] hosts1, String[] hosts2) {
		boolean isSame = true;

		if (hosts1.length != hosts2.length) {
			isSame = false;
		} else {
			for (String aHosts1 : hosts1) {
				boolean mark = false;
				for (int j = 0; j < hosts2.length; j++) {
					if (aHosts1.equals(hosts2[j])) {
						mark = true;
					}
				}
				if (!mark) {
					isSame = false;
					return false;
				}
			}
		}

		return isSame;
	}

    /**
     * Giant formatted print string.
     *
     * @param corner           the corner
     * @param currentCounter   the current counter
     * @param globalCoordinate the global coordinate
     * @param myGroupID        the my group id
     * @param val1             the val 1
     * @param val2             the val 2
     * @return the string
     */
    public static String giantFormattedPrint(int[] corner, int[] currentCounter, int[] globalCoordinate,
											 int[] myGroupID, int val1, int val2) {
		String retString = String.format(
				"gl co:%03d,%03d,%03d,%03d co: %03d,%03d,%03d,%03d ctr: %03d,%03d,%03d,%03d grp id: %03d,%03d,%03d,%03d v1: %010d v2: %010d\n",
				globalCoordinate[0], globalCoordinate[1],
				globalCoordinate[2], globalCoordinate[3], corner[0],
				corner[1], corner[2], corner[3],
				currentCounter[0], currentCounter[1],
				currentCounter[2], currentCounter[3], myGroupID[0],
				myGroupID[1], myGroupID[2], myGroupID[3],
				val1, val2);

		return retString;
	}

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {
		String input = "(1-3,4-5,4-7),(61-34,34-25,24-27),(11-3,4-15)";
		parseBoundaryBox(input);
	}

    /**
     * The enum Operator.
     */
    public enum Operator {
        /**
         * Average operator.
         */
        average, /**
         * Simple max operator.
         */
        simpleMax, /**
         * Max operator.
         */
        max, /**
         * Simple median operator.
         */
        simpleMedian, /**
         * Median operator.
         */
        median, /**
         * Nulltest operator.
         */
        nulltest, /**
         * Opt unknown operator.
         */
        optUnknown
	}

    /**
     * The enum Part mode.
     */
    public enum PartMode {
        /**
         * Proportional part mode.
         */
        proportional, /**
         * Record part mode.
         */
        record, /**
         * Calculated part mode.
         */
        calculated
	}

    /**
     * The enum Placement mode.
     */
    public enum PlacementMode {
        /**
         * Roundrobin placement mode.
         */
        roundrobin, /**
         * Sampling placement mode.
         */
        sampling, /**
         * Implicit placement mode.
         */
        implicit
	}

    /**
     * The enum Multi file mode.
     */
    public enum MultiFileMode {
        /**
         * Combine multi file mode.
         */
        combine, /**
         * Concat multi file mode.
         */
        concat
	}
}

/*
 * Location:
 * /Users/mkbowen/Desktop/MerraTest.jar!/edu/gmu/stc/merra/hadoop/Utils.class
 * Java compiler version: 6 (50.0) JD-Core Version: 0.7.1
 */