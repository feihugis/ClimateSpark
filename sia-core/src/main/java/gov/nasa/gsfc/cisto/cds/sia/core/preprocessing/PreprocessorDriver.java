package gov.nasa.gsfc.cisto.cds.sia.core.preprocessing;

import gov.nasa.gsfc.cisto.cds.sia.core.config.ConfigParameterKeywords;
import gov.nasa.gsfc.cisto.cds.sia.core.config.SiaConfiguration;
import gov.nasa.gsfc.cisto.cds.sia.core.config.SiaConfigurationFactory;
import gov.nasa.gsfc.cisto.cds.sia.core.config.SiaConfigurationUtils;
import gov.nasa.gsfc.cisto.cds.sia.core.config.UserProperties;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities.Merra2FilePathMetadata;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities.Merra2Metadata;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities.Merra2VariableMetadata;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities.MerraFilePathMetadata;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities.MerraMetadata;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities.MerraVariableMetadata;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities.SiaMetadata;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetparsers.Merra2Parser;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetparsers.SiaParser;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetparsers.SiaParserFactory;
import gov.nasa.gsfc.cisto.cds.sia.core.variableentities.SiaVariableEntity;
import gov.nasa.gsfc.cisto.cds.sia.hibernate.DAOImpl;
import gov.nasa.gsfc.cisto.cds.sia.hibernate.HibernateUtil;
import gov.nasa.gsfc.cisto.cds.sia.hibernate.PhysicalNameStrategyImpl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.hibernate.Session;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * The type Preprocessor driver.
 */
public class PreprocessorDriver {

    private static final Log LOG = LogFactory.getLog(PreprocessorDriver.class);


    public void preprocessing(String[] args) throws Exception {
      String jobType = "preprocessor";
      SiaConfigurationFactory siaConfigurationFactory = new SiaConfigurationFactory();
      SiaConfiguration preprocessorConfiguration = siaConfigurationFactory.getSiaConfiguration(jobType);
      preprocessorConfiguration.buildBaseConfiguration(args);

      Configuration hadoopConf = new Configuration();
      SiaConfigurationUtils.addKeysToHadoopConfiguration(preprocessorConfiguration.getBaseConfiguration(), hadoopConf);
      String datasetName = hadoopConf.get(ConfigParameterKeywords.DATASET_NAME);
      String dirPath = hadoopConf.get(ConfigParameterKeywords.INPUT_PATH);
      SiaParser siaParser = SiaParserFactory.getSiaParser(datasetName);
      List<FileStatus> fileList = siaParser.recursiveFileList(dirPath);

      //Create Hibernate session
      //PhysicalNameStrategyImpl physicalNameStrategy = new PhysicalNameStrategyImpl(tableName);
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
      DAOImpl<SiaMetadata> dao = new DAOImpl<SiaMetadata>();
      dao.setSession(session);

      siaParser.addAllCollectionToDB(fileList, dao, hadoopConf);
      session.close();
      hibernateUtil.closeSession();
      hibernateUtil.closeSessionFactory();

    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
        /*String jobType = "preprocessor";
        SiaConfigurationFactory siaConfigurationFactory = new SiaConfigurationFactory();
        SiaConfiguration preprocessorConfiguration = siaConfigurationFactory.getSiaConfiguration(jobType);
        preprocessorConfiguration.buildBaseConfiguration(args);
        UserProperties userProperties = preprocessorConfiguration.buildUserProperties();
        SiaParser siaParser = SiaParserFactory.getSiaParser(userProperties.getDatasetName());
        String directoryPath = userProperties.getInputPath();
        List<FileStatus> fileList = siaParser.recursiveFileList(directoryPath);
        siaParser.addAllCollectionsToDb(fileList);*/

        PreprocessorDriver preprocessorDriver = new PreprocessorDriver();
        preprocessorDriver.preprocessing(args);

        System.exit(0);
    }
}