package gov.nasa.gsfc.cisto.cds.sia.core.config;

import com.google.gson.Gson;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * The type Sia configuration utils.
 */
public class SiaConfigurationUtils {

    private static final Log LOG = LogFactory.getLog(SiaConfigurationUtils.class);

    /**
     * Serialize object string.
     *
     * @param object the object
     * @return the string
     */
    public static String serializeObject(Object object) {
        Gson gson = new Gson();
        String serializedObject = gson.toJson(object);
        return serializedObject;
    }

    /**
     * Deserialize object object.
     *
     * @param serializedObject      the serialized object
     * @param serializedObjectClass the serialized object class
     * @return the object
     */
    public static Object deserializeObject(String serializedObject, Class serializedObjectClass) {
        Gson gson = new Gson();
        Object deserializedObject = gson.fromJson(serializedObject, serializedObjectClass);
        return deserializedObject;
    }

    /**
     * Validate keys.
     *
     * @param configuration the configuration
     * @param jobType       the job type
     */
    public static void validateKeys(Configuration configuration, String jobType) {
        ArrayList<String> propFileKeys = getPropFileKeys(configuration);
        ArrayList<String> mandatoryKeys = getMandatoryKeys(jobType);

        for (String mandatoryKey : mandatoryKeys) {
            if (!propFileKeys.contains(mandatoryKey)) {
                missingKey(mandatoryKey);
                System.exit(1);
            }
            LOG.info(mandatoryKey + " property successfully validated. Value is " + configuration.getString(mandatoryKey));
        }
    }

    /**
     * Add keys to hadoop configuration.
     *
     * @param siaBaseConfig the sia base config
     * @param hadoopConfig  the hadoop config
     */
    public static void addKeysToHadoopConfiguration(CompositeConfiguration siaBaseConfig, org.apache.hadoop.conf.Configuration hadoopConfig) {
        ArrayList<String> propFileKeys = getPropFileKeys(siaBaseConfig);

        for(String key : propFileKeys) {
            LOG.info("Key: " + key + " value: " + siaBaseConfig.getString(key) + " set in hadoop configuration from sia.properties file.");
            hadoopConfig.set(key, siaBaseConfig.getString(key));
        }
    }

    private static void missingKey(String key) {
        LOG.error("Properties file incorrectly configured. The following key is missing: " + key);
        System.out.println("Properties file incorrectly configured. The following key is missing: " + key);
    }

    private static ArrayList<String> getPropFileKeys(Configuration configuration) {
        Iterator<String> keysFromPropFile = configuration.getKeys();
        ArrayList<String> propFileKeys = new ArrayList<String>();
        while(keysFromPropFile.hasNext()) {
            propFileKeys.add(keysFromPropFile.next());
        }

        return propFileKeys;
    }

    private static ArrayList<String> getMandatoryKeys(String jobType) {
        ArrayList<String> mandatoryKeys = new ArrayList<String>();
        if (jobType.equalsIgnoreCase("preprocessor")) {
            for (PreprocessorKeys key : PreprocessorKeys.values()) {
                mandatoryKeys.add(key.keyName);
            }
        }
        else if (jobType.equalsIgnoreCase("indexer")) {
            for (IndexerKeys key : IndexerKeys.values()) {
                mandatoryKeys.add(key.keyName);
            }
        }
        else if (jobType.equalsIgnoreCase("mapreducer")) {
            for (MapreducerKeys key : MapreducerKeys.values()) {
                mandatoryKeys.add(key.keyName);
            }
        }

        return mandatoryKeys;
    }

    private enum PreprocessorKeys {
        /**
         * Dataset name preprocessor keys.
         */
        DATASET_NAME("dataset.name"),
        /**
         * Job name preprocessor keys.
         */
        JOB_NAME("job.name"),
        /**
         * Input path preprocessor keys.
         */
        INPUT_PATH("input.path"),

        HIBERNATE_DRIEVER(ConfigParameterKeywords.HIBERNATE_DRIEVER),

        HIBERNATE_URL(ConfigParameterKeywords.HIBERNATE_URL),

        HIBERNATE_USER(ConfigParameterKeywords.HIBERNATE_USER),

        HIBERNATE_PASS(ConfigParameterKeywords.HIBERNATE_PASS),

        HIBERNATE_DIALECT(ConfigParameterKeywords.HIBERNATE_DIALECT),

        HIBERNATE_HBM2DDL_AUTO(ConfigParameterKeywords.HIBERNATE_HBM2DDL_AUTO),

        TEMPORAL_RESOLUTION(ConfigParameterKeywords.TEMPORAL_RESOLUTION),

        TEMPORAL_RANGE_START(ConfigParameterKeywords.TEMPORAL_RANGE_START),

        TEMPORAL_RANGE_END(ConfigParameterKeywords.TEMPORAL_RANGE_END),

        RAW_DATA_FORMAT(ConfigParameterKeywords.RAW_DATA_FORMAT),

        STATISTICAL_INTERVAL_TYPE(ConfigParameterKeywords.STATISTICAL_INTERVAL_TYPE)
        ;


        /**
         * Gets key name.
         *
         * @return the key name
         */
        public String getKeyName() {
            return keyName;
        }

        private String keyName;


        PreprocessorKeys(String keyName) {
            this.keyName = keyName;
        }
    }

    private enum IndexerKeys {
        /**
         * Dataset name indexer keys.
         */
        DATASET_NAME("dataset.name"),
        /**
         * Collection name indexer keys.
         */
        COLLECTION_NAME("collection.name"),
        /**
         * Job name indexer keys.
         */
        JOB_NAME("job.name"),
        /**
         * Input path indexer keys.
         */
        INPUT_PATH("input.path"),
        /**
         * Output path indexer keys.
         */
        OUTPUT_PATH("output.path"),
        /**
         * File extension indexer keys.
         */
        FILE_EXTENSION("file.extension"),
        /**
         * Variables indexer keys.
         */
        VARIABLES("variable.names"),
        /**
         * Framework indexer keys.
         */
        FRAMEWORK("mapreduce.framework.name"),

        /**
         * Files per map task indexer keys.
         */
        FILES_PER_MAP_TASK("files.per.map.task"),

        HIBERNATE_DRIEVER(ConfigParameterKeywords.HIBERNATE_DRIEVER),

        HIBERNATE_URL(ConfigParameterKeywords.HIBERNATE_URL),

        HIBERNATE_USER(ConfigParameterKeywords.HIBERNATE_USER),

        HIBERNATE_PASS(ConfigParameterKeywords.HIBERNATE_PASS),

        HIBERNATE_DIALECT(ConfigParameterKeywords.HIBERNATE_DIALECT),

        HIBERNATE_HBM2DDL_AUTO(ConfigParameterKeywords.HIBERNATE_HBM2DDL_AUTO),

        /**
         * The Year start.
         */
        // Spatiotemporal Input Parameters
        YEAR_START("year.start"),
        /**
         * Year end mapreducer keys.
         */
        YEAR_END("year.end"),
        /**
         * Month start mapreducer keys.
         */
        MONTH_START("month.start"),
        /**
         * Month end mapreducer keys.
         */
        MONTH_END("month.end"),
        /**
         * Day start mapreducer keys.
         */
        DAY_START("day.start"),
        /**
         * Day end mapreducer keys.
         */
        DAY_END("day.end"),
        /**
         * Hour start mapreducer keys.
         */
        HOUR_START("hour.start"),
        /**
         * Hour end mapreducer keys.
         */
        HOUR_END("hour.end"),
        /**
         * Height start mapreducer keys.
         */
        HEIGHT_START("height.start"),
        /**
         * Height end mapreducer keys.
         */
        HEIGHT_END("height.end"),
        /**
         * Lat start mapreducer keys.
         */
        LAT_START("lat.start"),
        /**
         * Lat end mapreducer keys.
         */
        LAT_END("lat.end"),
        /**
         * Lon start mapreducer keys.
         */
        LON_START("lon.start"),
        /**
         * Lon end mapreducer keys.
         */
        LON_END("lon.end");


        /**
         * Gets key name.
         *
         * @return the key name
         */
        public String getKeyName() {
            return keyName;
        }

        private String keyName;


        IndexerKeys(String keyName) {
            this.keyName = keyName;
        }
    }

    private enum MapreducerKeys {
        /**
         * Dataset name mapreducer keys.
         */
        DATASET_NAME("dataset.name"),
        /**
         * Collection name mapreducer keys.
         */
        COLLECTION_NAME("collection.name"),
        /**
         * Job name mapreducer keys.
         */
        JOB_NAME("job.name"),
        /**
         * Input path mapreducer keys.
         */
        INPUT_PATH("input.path"),
        /**
         * Output path mapreducer keys.
         */
        OUTPUT_PATH("output.path"),
        /**
         * File extension mapreducer keys.
         */
        FILE_EXTENSION("file.extension"),
        /**
         * Variables mapreducer keys.
         */
        VARIABLES("variable.names"),
        /**
         * Framework mapreducer keys.
         */
        FRAMEWORK("mapreduce.framework.name"),
        /**
         * Xml hibernate table mapping file path mapreducer keys.
         */
        // XML_HIBERNATE_TABLE_MAPPING_FILE_PATH("xml.hibernate.table.mapping.file.path"),
        /**
         * Threads per node mapreducer keys.
         */
        // THREADS_PER_NODE("threads.per.node"),
        /**
         * Analytics operation mapreducer keys.
         */
        ANALYTICS_OPERATION("analytics.operation"),
        /**
         * Number reducers mapreducer keys.
         */
        // NUMBER_REDUCERS("number.reducers"),

        /**
         * The Year start.
         */
        // Spatiotemporal Input Parameters
        YEAR_START("year.start"),
        /**
         * Year end mapreducer keys.
         */
        YEAR_END("year.end"),
        /**
         * Month start mapreducer keys.
         */
        MONTH_START("month.start"),
        /**
         * Month end mapreducer keys.
         */
        MONTH_END("month.end"),
        /**
         * Day start mapreducer keys.
         */
        DAY_START("day.start"),
        /**
         * Day end mapreducer keys.
         */
        DAY_END("day.end"),
        /**
         * Hour start mapreducer keys.
         */
        HOUR_START("hour.start"),
        /**
         * Hour end mapreducer keys.
         */
        HOUR_END("hour.end"),
        /**
         * Height start mapreducer keys.
         */
        HEIGHT_START("height.start"),
        /**
         * Height end mapreducer keys.
         */
        HEIGHT_END("height.end"),
        /**
         * Lat start mapreducer keys.
         */
        LAT_START("lat.start"),
        /**
         * Lat end mapreducer keys.
         */
        LAT_END("lat.end"),
        /**
         * Lon start mapreducer keys.
         */
        LON_START("lon.start"),
        /**
         * Lon end mapreducer keys.
         */
        LON_END("lon.end"),

        HIBERNATE_DRIEVER(ConfigParameterKeywords.HIBERNATE_DRIEVER),

        HIBERNATE_URL(ConfigParameterKeywords.HIBERNATE_URL),

        HIBERNATE_USER(ConfigParameterKeywords.HIBERNATE_USER),

        HIBERNATE_PASS(ConfigParameterKeywords.HIBERNATE_PASS),

        HIBERNATE_DIALECT(ConfigParameterKeywords.HIBERNATE_DIALECT),

        HIBERNATE_HBM2DDL_AUTO(ConfigParameterKeywords.HIBERNATE_HBM2DDL_AUTO)
      ;


        /**
         * Gets key name.
         *
         * @return the key name
         */
        public String getKeyName() {
            return keyName;
        }

        private String keyName;


        MapreducerKeys(String keyName) {
            this.keyName = keyName;
        }
    }
}
