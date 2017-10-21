package gov.nasa.gsfc.cisto.cds.sia.core.preprocessing;

import gov.nasa.gsfc.cisto.cds.sia.core.config.SiaConfiguration;
import gov.nasa.gsfc.cisto.cds.sia.core.config.SiaConfigurationFactory;
import gov.nasa.gsfc.cisto.cds.sia.core.config.UserProperties;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetparsers.SiaParser;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetparsers.SiaParserFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;

import java.io.File;
import java.util.List;

/**
 * The type Preprocessor driver.
 */
public class PreprocessorDriver {

    private static final Log LOG = LogFactory.getLog(PreprocessorDriver.class);

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
        String jobType = "preprocessor";
        SiaConfigurationFactory siaConfigurationFactory = new SiaConfigurationFactory();
        SiaConfiguration preprocessorConfiguration = siaConfigurationFactory.getSiaConfiguration(jobType);
        preprocessorConfiguration.buildBaseConfiguration(args);
        UserProperties userProperties = preprocessorConfiguration.buildUserProperties();
        SiaParser siaParser = SiaParserFactory.getSiaParser(userProperties.getDatasetName());
        String directoryPath = userProperties.getInputPath();
        List<FileStatus> fileList = siaParser.recursiveFileList(directoryPath);
        siaParser.addAllCollectionsToDb(fileList);

      System.exit(0);
    }
}