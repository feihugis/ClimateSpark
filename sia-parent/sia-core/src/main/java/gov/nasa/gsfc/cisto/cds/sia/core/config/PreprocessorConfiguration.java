package gov.nasa.gsfc.cisto.cds.sia.core.config;

import gov.nasa.gsfc.cisto.cds.sia.core.collection.SiaCollection;
import gov.nasa.gsfc.cisto.cds.sia.core.dataset.SiaDataset;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.File;

/**
 * The type Preprocessor configuration.
 */
public class PreprocessorConfiguration extends CompositeConfiguration implements SiaConfiguration {

    private CompositeConfiguration baseConfig;
    private boolean isBuilt = false;
    private static final Log LOG = LogFactory.getLog(PreprocessorConfiguration.class);
    private String jobType = "preprocessor";

    public void buildBaseConfiguration(String[] args) throws ConfigurationException {
        if(isBuilt) {
            return;
        }

        validateCommandLineInput(args);
        this.baseConfig = new CompositeConfiguration();
        File propertiesFile = new File(args[0]);

        if(propertiesFile.canRead()) {
            baseConfig.addConfiguration(new PropertiesConfiguration(propertiesFile));
            LOG.info("Preprocessor properties file successfully loaded from " + propertiesFile.getPath());
        } else {
            LOG.error("Could not find preprocessor properties file in " + propertiesFile.getPath());
            System.exit(1);
        }

        SiaConfigurationUtils.validateKeys(baseConfig, jobType);
        isBuilt = true;
    }

    public CompositeConfiguration getBaseConfiguration() {
        validateBaseConfigIsBuilt();

        return this.baseConfig;
    }

    public UserProperties buildUserProperties() {
        validateBaseConfigIsBuilt();

        String datasetName = baseConfig.getString("dataset.name");
        String jobName = baseConfig.getString("job.name");

        UserProperties userProperties = new UserProperties.UserPropertiesBuilder(datasetName, jobName)
                .inputPath(baseConfig.getString("input.path"))
                .build();

        return userProperties;
    }

    public SpatiotemporalFilters buildSpatiotemporalFilters(String dateType) {
      throw new UnsupportedOperationException("Preprocessor does not support spatiotemporal filters.");
    }

    public SiaDataset buildSIADataset(UserProperties userProperties) {
        throw new UnsupportedOperationException("Preprocessor does not support sia datasets.");
    }

    public SiaCollection buildSIACollection(UserProperties userProperties) {
        throw new UnsupportedOperationException("Preprocessor does not support sia collections.");
    }


    public Job createHadoopJob(Configuration hadoopConfiguration, UserProperties userProperties) {
        throw new UnsupportedOperationException("Preprocessor does not support hadoop jobs.");
    }


    private void validateBaseConfigIsBuilt() {
        if (!isBuilt) {
            LOG.error("Base configuration not built yet.");
            System.exit(2);
        }
    }

    private void validateCommandLineInput(String args[]) {
        if(args.length != 1) {
            System.err.println("Usage: <sia-preprocessor-properties-file-path");
            LOG.error("Usage: <sia-preprocessor-properties-file-path");
            System.exit(2);
        }
    }
}
