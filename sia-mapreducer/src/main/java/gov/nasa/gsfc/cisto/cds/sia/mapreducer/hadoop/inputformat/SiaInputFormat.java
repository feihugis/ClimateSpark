package gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.inputformat;

import gov.nasa.gsfc.cisto.cds.sia.core.HibernateUtils;
import gov.nasa.gsfc.cisto.cds.sia.core.common.DAOImpl;
import gov.nasa.gsfc.cisto.cds.sia.core.common.FileUtils;
import gov.nasa.gsfc.cisto.cds.sia.core.config.ConfigParameterKeywords;
import gov.nasa.gsfc.cisto.cds.sia.core.config.GeneralClassLoader;
import gov.nasa.gsfc.cisto.cds.sia.core.config.SiaConfigurationUtils;
import gov.nasa.gsfc.cisto.cds.sia.core.config.SpatiotemporalFilters;
import gov.nasa.gsfc.cisto.cds.sia.core.config.UserProperties;
import gov.nasa.gsfc.cisto.cds.sia.core.io.SiaChunk;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities.*;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetparsers.SiaFilePathCompositeKey;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetparsers.SiaVariableCompositeKey;
import gov.nasa.gsfc.cisto.cds.sia.core.variableentities.SiaVariableEntity;
import gov.nasa.gsfc.cisto.cds.sia.core.variableentities.SiaVariableEntityFactory;
import gov.nasa.gsfc.cisto.cds.sia.hibernate.HibernateUtil;
import gov.nasa.gsfc.cisto.cds.sia.hibernate.PhysicalNameStrategyImpl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.hibernate.Session;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.inputformat.SiaInputSplitFactory.integerListToIntArray;
import static gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.inputformat.SiaInputSplitFactory.stringListToStringArray;
import static gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.inputformat.SiaInputSplitFactory.stringToIntArray;


/**
 * Created by Fei Hu on 9/12/16.
 */
public class SiaInputFormat extends FileInputFormat {
  public static final Log LOG = LogFactory.getLog(SiaInputFormat.class);

  //private String indexDBConfig = "merra_indexer_db.cfg.xml";
  private int[] queryCorner = null;
  private int[] queryShape = null;
  private Configuration conf;

  /**
   *
   * @param job
   * @return
   * @throws IOException
   */
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    conf = job.getConfiguration();
    List<InputSplit> inputSplits = new ArrayList<InputSplit>();

    SpatiotemporalFilters spatiotemporalFilters = (SpatiotemporalFilters) SiaConfigurationUtils.
        deserializeObject(conf.get(ConfigParameterKeywords.spatiotemporalFiltersSerialized),
                          SpatiotemporalFilters.class);

    int startDate = spatiotemporalFilters.getStartDate();
    int endDate = spatiotemporalFilters.getEndDate();

    String[] varNames = conf.get(ConfigParameterKeywords.VARIABLE_NAMES).split(",");
    String[] inputVarNames = new String[varNames.length];
    for (int i = 0; i < inputVarNames.length; i++) {
      inputVarNames[i] = varNames[i].trim();
    }

    String collectionName = conf.get(ConfigParameterKeywords.COLLECTION_NAME);
    String datasetName = conf.get(ConfigParameterKeywords.DATASET_NAME);
    int threadNumPerNode = conf.getInt(ConfigParameterKeywords.THREADS_PER_NODE, 12);

    queryCorner = spatiotemporalFilters.getStartSpatialBounding();
    queryShape = new int[queryCorner.length];
    int[] endCorner = spatiotemporalFilters.getEndSpatialBounding();
    for (int i = 0; i < endCorner.length; i++) {
      queryShape[i] = endCorner[i] - queryCorner[i] + 1;
    }

    for (String varName : inputVarNames) {
      try {
        inputSplits.addAll(getSplits(job, varName, collectionName, datasetName, threadNumPerNode,
                                     startDate, endDate));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    return inputSplits;
  }

  public List<InputSplit> getSplits(JobContext job, String varName, String collectionName,
                                    String datasetName, int threadNumPerNode,
                                    final int startDate, final int endDate)
      throws Exception {
    String tableName = String.format("%s_%s", varName.toString().toLowerCase(),
                                     collectionName.toLowerCase());
    Class entityClass = SiaVariableEntityFactory.getSIAVariableEntity(datasetName).getClass();

    PhysicalNameStrategyImpl physicalNameStrategy = new PhysicalNameStrategyImpl(tableName);
    HibernateUtil hibernateUtil = new HibernateUtil();

    hibernateUtil.createSessionFactoryWithPhysicalNamingStrategy(conf,
                                                                 physicalNameStrategy,
                                                                 entityClass);
    Session session = hibernateUtil.getSession();
    gov.nasa.gsfc.cisto.cds.sia.hibernate.DAOImpl<SiaVariableEntity>
        dao = new gov.nasa.gsfc.cisto.cds.sia.hibernate.DAOImpl<SiaVariableEntity>();
    dao.setSession(session);

    String query = String.format("from %s where temporal_component >= %d and temporal_component <= %d order by temporal_component",
                                 tableName,
                                 startDate,
                                 endDate);

    List<SiaVariableEntity> merraVariableEntityRetrieved = (List<SiaVariableEntity>) dao.findByQuery(query, entityClass);


    SiaVariableMetadata siaVariableMetadata = getSiaVariableMetaData(varName, datasetName, collectionName, job.getConfiguration());
    String dataType = siaVariableMetadata.getDataType();
    final int[] chunkShape = integerListToIntArray(siaVariableMetadata.getChunkSizes());

    List<SiaChunk> siaChunks = new ArrayList<SiaChunk>();

    hibernateUtil.closeSession();
    hibernateUtil.shutdown();

    HashMap<Integer, String> temporalToFilePathMap = getTemporalToFilePathMap(datasetName, job.getConfiguration(), collectionName, startDate, endDate);

    for (SiaVariableEntity entity : merraVariableEntityRetrieved) {
      int[] corner = stringToIntArray(entity.getCorner(), ",");

      if(!SiaInputSplitFactory.isIntersected(corner, chunkShape, queryCorner, queryShape)) continue;

      String[] dimensions = stringListToStringArray(siaVariableMetadata.getDimensionOrder());

      SiaChunk siaChunk = new SiaChunk(corner, chunkShape,
                                       dimensions,
                                       entity.getByteOffset(), entity.getByteLength(),
                                       entity.getCompressionCode(), entity.getBlockHosts().split(";") ,
                                       dataType, varName,
                                       temporalToFilePathMap.get(entity.getTemporalComponent()),
                                       entity.getTemporalComponent(), "0");
      siaChunks.add(siaChunk);
    }

    List<SiaInputSplit> siaInputSplits = new ArrayList<SiaInputSplit>();
    siaInputSplits.addAll(SiaInputSplitFactory.genSIAInputSplitByHosts(siaChunks));

    List<InputSplit> groupedSiaInputSplits = new ArrayList<InputSplit>();
    groupedSiaInputSplits.addAll(SiaInputSplitFactory.genGroupedSIAInputSplit(siaInputSplits, threadNumPerNode));

    hibernateUtil.closeSession();
    hibernateUtil.shutdown();
    return groupedSiaInputSplits;
  }

  private Object getSiaFilePathMetadataClass(String datasetName) throws Exception {
    String siaFilePathMetadataClass = "";
    if (datasetName.equals("MERRA")) {
      siaFilePathMetadataClass = "gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities.MerraFilePathMetadata";
    } else if (datasetName.equals("MERRA2")) {
      siaFilePathMetadataClass = "gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities.Merra2FilePathMetadata";
    } else {
      throw new IllegalArgumentException("Please check the input dataset in the property file");
    }

    return GeneralClassLoader.loadObject(siaFilePathMetadataClass, null, null);
  }

  private SiaVariableMetadata getSiaVariableMetaData(String varName,
                                                     String dataset, String collectionName, Configuration hadoopConf)
      throws IOException {

    HibernateUtil hibernateUtil = new HibernateUtil();
    List<Class> mappingClassList = new ArrayList<Class>();
    mappingClassList.add(MerraMetadata.class);
    mappingClassList.add(MerraVariableMetadata.class);
    mappingClassList.add(MerraFilePathMetadata.class);
    mappingClassList.add(Merra2Metadata.class);
    mappingClassList.add(Merra2VariableMetadata.class);
    mappingClassList.add(Merra2FilePathMetadata.class);

    hibernateUtil.createSessionFactory(hadoopConf, mappingClassList);
    Session session = hibernateUtil.getSession();
    gov.nasa.gsfc.cisto.cds.sia.hibernate.DAOImpl<SiaVariableMetadata>
        dao = new gov.nasa.gsfc.cisto.cds.sia.hibernate.DAOImpl<SiaVariableMetadata>();
    dao.setSession(session);

    SiaVariableMetadata siaVariableMetadata;
    String datasetType = dataset.toLowerCase();

    SiaVariableCompositeKey siaVariableCompositeKey = new SiaVariableCompositeKey();
    siaVariableCompositeKey.setCollectionName(collectionName.toLowerCase());
    siaVariableCompositeKey.setVariableName(varName);

    if (datasetType.equals("merra")) {
      siaVariableMetadata = (MerraVariableMetadata) dao.findVariableByKey(MerraVariableMetadata.class, siaVariableCompositeKey);
    } else if (datasetType.equals("merra2")) {
      siaVariableMetadata = (Merra2VariableMetadata) dao.findVariableByKey(Merra2VariableMetadata.class, siaVariableCompositeKey);
    } else {
      throw new IOException("Could not find the SiaVariableMetadata for this dataset");
    }

    return siaVariableMetadata;
  }

  public HashMap<Integer, String> getTemporalToFilePathMap(String datasetName, Configuration hadoopConf, String collectionName, int startDate, int endDate) {
    HibernateUtil hibernateUtil = new HibernateUtil();
    List<Class> mappingClassList = new ArrayList<Class>();
    mappingClassList.add(MerraMetadata.class);
    mappingClassList.add(MerraVariableMetadata.class);
    mappingClassList.add(MerraFilePathMetadata.class);
    mappingClassList.add(Merra2Metadata.class);
    mappingClassList.add(Merra2VariableMetadata.class);
    mappingClassList.add(Merra2FilePathMetadata.class);
    mappingClassList.add(SiaFilePathMetadata.class);

    hibernateUtil.createSessionFactory(hadoopConf, mappingClassList);
    Session session = hibernateUtil.getSession();
    gov.nasa.gsfc.cisto.cds.sia.hibernate.DAOImpl<SiaFilePathMetadata>
        dao = new gov.nasa.gsfc.cisto.cds.sia.hibernate.DAOImpl<SiaFilePathMetadata>();
    dao.setSession(session);

    String tableName = "";
    Class mappingClass = null;
    if (datasetName.toLowerCase().equals("merra2")) {

      tableName = ConfigParameterKeywords.MERRA2_FILE_PATH_METADATA_TABLE_NAME;
      mappingClass = Merra2FilePathMetadata.class;
    } else  if (datasetName.toLowerCase().equals("merra")) {
        tableName = ConfigParameterKeywords.MERRA_FILE_PATH_METADATA_TABLE_NAME;
        mappingClass = MerraFilePathMetadata.class;
    } else {
        LOG.error("Do not support this kind of dataset file path metadata : " + datasetName);
    }

    String queryFilePath = String.format("from %s where collection_name = '%s' "
                                         + "and temporal_key >= %d "
                                         + "and temporal_key <= %d",
                                         tableName,
                                         collectionName.toLowerCase(),
                                         startDate,
                                         endDate);

    LOG.info("SQL for querying file path : " + queryFilePath);

    List<SiaFilePathMetadata> filePathMetadataList = (List<SiaFilePathMetadata>) dao.findByQuery(queryFilePath, mappingClass);

    HashMap<Integer, String> temporalToFilePathMap = new HashMap<Integer, String>();
    for (SiaFilePathMetadata siaFilePathMetadata : filePathMetadataList) {
      temporalToFilePathMap.put(siaFilePathMetadata.getSiaFilePathCompositeKey().getTemporalKey(), siaFilePathMetadata.getFilePath());
    }

    LOG.info("************ Number of file path : " + temporalToFilePathMap.size());

    return temporalToFilePathMap;
  }


  @Override
  public org.apache.hadoop.mapreduce.RecordReader createRecordReader(InputSplit inputSplit,
                                                                     TaskAttemptContext taskAttemptContext) {
    return new GroupedSiaRecordReader();
  }

  private static List<FileStatus> filterInputFiles(FileSystem fs, Path inputFilePath, int startTime,
                                                   int endTIme) throws Exception {
    final int endTime_f = endTIme;
    final int startTime_f = startTime;
    PathFilter MerraPathFilter = new PathFilter() {
      public boolean accept(Path path) {
        Configuration hconf = new Configuration();
        try {
          FileSystem fs = FileSystem.get(hconf);
          if (fs.isDirectory(path)) {
            return true;
          } else {
            if (path.toString().endsWith("nc4") || path.toString().endsWith("hdf")) {
              String[] strs = path.toString().split("\\.");
              int time = Integer.parseInt(strs[strs.length-2]);
              return time <= endTime_f && time >= startTime_f;
            } else {
              return false;
            }
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
        return false;
      }
    };

    ArrayList<FileStatus> fileList = new ArrayList<FileStatus>();
    FileUtils.getFileList(fs, inputFilePath, MerraPathFilter, fileList, true);
    return fileList;
  }

  public static void main(String[] args) throws Exception {
    Configuration hconf = new Configuration();
    hconf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive",true);
    hconf.set("fs.file.impl","org.apache.hadoop.fs.LocalFileSystem");
    hconf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    final int startTime = 19800101;
    final int endTime = 20161231;
    FileSystem fs = FileSystem.get(hconf);

    List<FileStatus> fileList = new ArrayList<FileStatus>();
    fileList = SiaInputFormat.filterInputFiles(fs, new Path("/Users/feihu/Documents/Data/M2T1NXINT/"), 19800101, 19991231);
    for (FileStatus fileStatus : fileList) {
      System.out.print(fileStatus.getPath().toString() + "\n");
    }
  }

}
