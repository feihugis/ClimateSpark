package gov.nasa.gsfc.cisto.cds.sia.core.config;

import gov.nasa.gsfc.cisto.cds.sia.core.collection.SiaCollection;
import gov.nasa.gsfc.cisto.cds.sia.core.dataset.SiaDataset;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by Fei Hu on 2/21/17.
 */
public class HadoopConfiguration {
  private Configuration hadoopConf;


    /**
     * Instantiates a new Hadoop configuration.
     *
     * @param args the args
     * @throws ConfigurationException the configuration exception
     */
    public HadoopConfiguration(String[] args) throws ConfigurationException {
    String jobType = "mapreducer";
    SiaConfigurationFactory siaConfigurationFactory = new SiaConfigurationFactory();
    SiaConfiguration mapreducerConfiguration = siaConfigurationFactory.getSiaConfiguration(jobType);
    mapreducerConfiguration.buildBaseConfiguration(args);

    // Create the user properties object and then bundle it up so it can be put into hadoop configuration
    UserProperties userProperties = mapreducerConfiguration.buildUserProperties();

    String userPropertiesSerialized = SiaConfigurationUtils.serializeObject(userProperties);



    // Create the dataset and collection objects and then bundle them up so they can be put into hadoop configuration
    SiaDataset siaDataset = mapreducerConfiguration.buildSIADataset(userProperties);
    String siaDatasetSerialized = SiaConfigurationUtils.serializeObject(siaDataset);
    SiaCollection
        siaCollection = mapreducerConfiguration.buildSIACollection(userProperties);
    String siaCollectionSerialized = SiaConfigurationUtils.serializeObject(siaCollection);
    String siaDatasetClass = siaDataset.getClass().toString().split(" ")[1];
    String siaCollectionClass = siaCollection.getClass().toString().split(" ")[1];

    // Create the spatiotemporal filters object and then bundle it up so it can be put into hadoop configuration
    SpatiotemporalFilters spatiotemporalFilters = mapreducerConfiguration.buildSpatiotemporalFilters("monthly");
    String spatiotemporalFiltersSerialized = SiaConfigurationUtils.serializeObject(spatiotemporalFilters);

    // Setup the hadoop configuration
    this.hadoopConf  = new Configuration();
    SiaConfigurationUtils.addKeysToHadoopConfiguration(mapreducerConfiguration.getBaseConfiguration(), this.hadoopConf);
    this.hadoopConf.set(ConfigParameterKeywords.userPropertiesSerialized, userPropertiesSerialized);
    this.hadoopConf.set(ConfigParameterKeywords.spatiotemporalFiltersSerialized, spatiotemporalFiltersSerialized);
    this.hadoopConf.set(ConfigParameterKeywords.siaDatasetSerialized, siaDatasetSerialized);
    this.hadoopConf.set(ConfigParameterKeywords.siaCollectionSerialized, siaCollectionSerialized);
    this.hadoopConf.set(ConfigParameterKeywords.siaDatasetClass, siaDatasetClass);
    this.hadoopConf.set(ConfigParameterKeywords.siaCollectionClass, siaCollectionClass);
  }

    /**
     * Gets hadoop conf.
     *
     * @return the hadoop conf
     */
    public Configuration getHadoopConf() {
    return this.hadoopConf;
  }

}
