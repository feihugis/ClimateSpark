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
import org.hibernate.SessionFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.inputformat.SiaInputSplitFactory.integerListToIntArray;
import static gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.inputformat.SiaInputSplitFactory.stringListToStringArray;
import static gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.inputformat.SiaInputSplitFactory.stringToIntArray;


/**
 * Created by Fei Hu on 9/12/16.
 */
public class SiaInputFormat extends FileInputFormat {
  private String indexDBConfig = "merra_indexer_db.cfg.xml";
  private int[] queryCorner = null;
  private int[] queryShape = null;

  /**
   *
   * @param job
   * @return
   * @throws IOException
   */
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    Configuration conf = job.getConfiguration();
    List<InputSplit> inputSplits = new ArrayList<InputSplit>();

    UserProperties userProperties = (UserProperties) SiaConfigurationUtils.
        deserializeObject(conf.get(ConfigParameterKeywords.userPropertiesSerialized),
                          UserProperties.class);

    SpatiotemporalFilters spatiotemporalFilters = (SpatiotemporalFilters) SiaConfigurationUtils.
        deserializeObject(conf.get(ConfigParameterKeywords.spatiotemporalFiltersSerialized),
                          SpatiotemporalFilters.class);

    indexDBConfig = getIndexDBConfigFile(userProperties.getDatasetName());

    int startDate = spatiotemporalFilters.getStartDate();
    int endDate = spatiotemporalFilters.getEndDate();

    String[] varNames = userProperties.getVariableNames();
    String collectionName = userProperties.getCollectionName();
    String datasetName = userProperties.getDatasetName();

    queryCorner = spatiotemporalFilters.getStartSpatialBounding();
    queryShape = new int[queryCorner.length];
    int[] endCorner = spatiotemporalFilters.getEndSpatialBounding();
    for (int i = 0; i < endCorner.length; i++) {
      queryShape[i] = endCorner[i] - queryCorner[i] + 1;
    }


    for (String varName : varNames) {
      try {
        inputSplits.addAll(getSplits(job, varName, collectionName, datasetName, indexDBConfig, startDate, endDate));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    return inputSplits;
  }

  public List<InputSplit> getSplits(JobContext job, String varName, String collectionName, String datasetName, String indexDBConfig, final int startDate, final int endDate)
      throws Exception {
    String metadataHibernateConf = "sia_metadata_db.cfg.xml";
    SessionFactory sessionFactory1 = HibernateUtils.createSessionFactory(metadataHibernateConf);
    DAOImpl dao1 = new DAOImpl();
    Session session1 = sessionFactory1.openSession();
    dao1.setSession(session1);

    SiaVariableMetadata siaVariableMetadata = getSiaVariableMetaData(metadataHibernateConf, varName, datasetName, collectionName);
    String dataType = siaVariableMetadata.getDataType();
    final int[] chunkShape = integerListToIntArray(siaVariableMetadata.getChunkSizes());

    SessionFactory sessionFactory = HibernateUtils.createSessionFactory(indexDBConfig);
    DAOImpl dao = new DAOImpl();
    Session session = sessionFactory.openSession();
    dao.setSession(session);
    String tableName = varName.toLowerCase() + "_" + collectionName.toLowerCase();
    String query = String.format("from %s where temporal_component >= %d and temporal_component <= %d", tableName, startDate, endDate);
    System.out.println("Query: " + query);
    List<SiaVariableEntity> merraVariableEntityRetrieved = (List<SiaVariableEntity>) dao.findByQuery(query);

    List<SiaChunk> siaChunks = new ArrayList<SiaChunk>();
    for (SiaVariableEntity entity : merraVariableEntityRetrieved) {
      int[] corner = stringToIntArray(entity.getCorner(), ",");

      if(!SiaInputSplitFactory.isIntersected(corner, chunkShape, queryCorner, queryShape)) continue;

      String[] dimensions = stringListToStringArray(siaVariableMetadata.getDimensionOrder());

      System.out.println("Entity: " + entity.getByteLength());

      SiaFilePathCompositeKey siaFilePathCompositeKey = new SiaFilePathCompositeKey();
      siaFilePathCompositeKey.setCollectionName(collectionName);
      siaFilePathCompositeKey.setTemporalKey(Integer.toString(entity.getTemporalComponent()));

      Object siaFilePathMetadataObject = getSiaFilePathMetadataClass(datasetName);
      SiaFilePathMetadata siaFilePathMetadata = (SiaFilePathMetadata) dao1.findFilePathByKey(siaFilePathMetadataObject.getClass(), siaFilePathCompositeKey);

      SiaChunk siaChunk = new SiaChunk(corner, chunkShape,
                                       dimensions,
                                       entity.getByteOffset(), entity.getByteLength(),
                                       entity.getCompressionCode(), entity.getBlockHosts().split(";") ,
                                       dataType, varName,
                                       siaFilePathMetadata.getFilePath(),
                                       entity.getTemporalComponent(), "0");
      siaChunks.add(siaChunk);
    }

    List<InputSplit> siaInputSplits = new ArrayList<InputSplit>();
    siaInputSplits.addAll(SiaInputSplitFactory.genSIAInputSplitByHosts(siaChunks));
    return siaInputSplits;
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

  private String getIndexDBConfigFile(String datasetName) {
    if (datasetName.equals("MERRA")) return "merra_indexer_db.cfg.xml";
    if (datasetName.equals("MERRA2")) return "merra2_indexer_db.cfg.xml";
    throw new IllegalArgumentException("Please check the input dataset in the property file");
  }



  private SiaVariableMetadata getSiaVariableMetaData(String hibernateConf, String varName,
                                                     String dataset, String collectionName)
      throws IOException {
    SessionFactory sessionFactory = HibernateUtils.createSessionFactory(hibernateConf);  //sia_metadata_db.cfg.xml

    DAOImpl dao = new DAOImpl();
    Session session = sessionFactory.openSession();
    dao.setSession(session);
    SiaVariableMetadata siaVariableMetadata;
    String datasetType = dataset.toLowerCase();

    SiaVariableCompositeKey siaVariableCompositeKey = new SiaVariableCompositeKey();
    siaVariableCompositeKey.setCollectionName(collectionName);
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

  @Override
  public org.apache.hadoop.mapreduce.RecordReader createRecordReader(InputSplit inputSplit,
                                                                     TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    return new SiaRecordReader();
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
