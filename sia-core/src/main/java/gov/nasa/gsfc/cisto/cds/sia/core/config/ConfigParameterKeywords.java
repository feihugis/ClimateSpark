package gov.nasa.gsfc.cisto.cds.sia.core.config;

/**
 * The type Config parameter keywords.
 */
public final class ConfigParameterKeywords {
  /**
   * The constant spatiotemporalFiltersSerialized.
   */
  public static final String spatiotemporalFiltersSerialized = "spatiotemporalFiltersSerialized";
  /**
   * The constant userPropertiesSerialized.
   */
  public static final String userPropertiesSerialized = "userPropertiesSerialized";
  /**
   * The constant siaDatasetSerialized.
   */
  public static final String siaDatasetSerialized = "siaDatasetSerialized";
  /**
   * The constant siaCollectionSerialized.
   */
  public static final String siaCollectionSerialized = "siaCollectionSerialized";
  /**
   * The constant hibernateConfigSerialized.
   */
  public static final String hibernateConfigSerialized = "hibernateConfigSerialized";
  /**
   * The constant siaCollectionClass.
   */
  public static final String siaCollectionClass = "siaCollectionClass";
  /**
   * The constant siaDatasetClass.
   */
  public static final String siaDatasetClass = "siaDatasetClass";

  // ********* CONFIG PARAMETERS FROM sia.properties FILE **********

  /**
   * The constant datasetName.
   */
  public static final String datasetName = "datasetName";
  /**
   * The constant collectionName.
   */
  public static final String collectionName = "collectionName";
  /**
   * The constant jobType.
   */
  public static final String jobType = "jobType";
  /**
   * The constant basePackage.
   */
  public static final String basePackage = "basePackage";
  /**
   * The constant inputPath.
   */
  public static final String inputPath = "inputPath";
  /**
   * The constant outputPath.
   */
  public static final String outputPath = "outputPath";
  /**
   * The constant variableNames.
   */
  public static final String variableNames = "variableNames";

  public static final String VARIABLE_NAMES = "variable.names";

  public static final String FILES_PER_MAP_TASKS = "files.per.map.task";

  public static final String DATASET_NAME = "dataset.name";

  public static final String COLLECTION_NAME = "collection.name";

  public static final String FILE_EXTENSION = "file.extension";

  public static final String INPUT_PATH = "input.path";

  /**
   * The constant numberReducers.
   */
// Mapreduce specific parameters
  public static final String numberReducers = "numberReducers";
  /**
   * The constant analyticsOperation.
   */
  public static final String analyticsOperation = "analyticsOperation";


  /**
   * The constant validMin.
   */
  public static final String validMin = "validMin";
  /**
   * The constant validMax.
   */
  public static final String validMax = "validMax";

  //Hibernate
  public static final String HIBERNATE_DRIEVER = "hibernate.connection.driver_class";  //org.postgresql.Driver
  public static final String HIBERNATE_URL = "hibernate.connection.url";  //jdbc:postgresql://localhost:5432/hibernate_test
  public static final String HIBERNATE_USER = "hibernate.connection.username";
  public static final String HIBERNATE_PASS = "hibernate.connection.password";
  public static final String HIBERNATE_DIALECT = "hibernate.dialect";  //org.hibernate.dialect.PostgreSQL9Dialect
  public static final String HIBERNATE_HBM2DDL_AUTO = "hibernate.hbm2ddl.auto"; //update

  public static final String TEMPORAL_RESOLUTION = "temporal.resolution";
  public static final String TEMPORAL_RANGE_START = "temporal.range.start";
  public static final String TEMPORAL_RANGE_END = "temporal.range.end";
  public static final String RAW_DATA_FORMAT = "rawdataformat";
  public static final String STATISTICAL_INTERVAL_TYPE = "statistic.interval.type";

  public static final String  MERRA2_FILE_PATH_METADATA_TABLE_NAME = "merra2_file_path_metadata";

  public static final String  MERRA_FILE_PATH_METADATA_TABLE_NAME = "merra_file_path_metadata";

}
