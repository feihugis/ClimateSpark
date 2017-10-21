package gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop;

import gov.nasa.gsfc.cisto.cds.sia.core.collection.SiaCollection;
import gov.nasa.gsfc.cisto.cds.sia.core.config.*;
import gov.nasa.gsfc.cisto.cds.sia.core.dataset.SiaDataset;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Main driver for Map/Reduce index job
 * 
 * @author mkbowen
 *
 */
public class Driver extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(Driver.class);

	/**
	 * Main driver for MapReduce analytics
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		String classpath = System.getProperty("java.class.path");
		LOG.debug(">> CLASSPATH: \n" + classpath);

		int res = ToolRunner.run(new Configuration(), new Driver(), args);
		System.exit(res);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	public int run(String[] args) throws Exception {
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
		SiaCollection siaCollection = mapreducerConfiguration.buildSIACollection(userProperties);
		String siaCollectionSerialized = SiaConfigurationUtils.serializeObject(siaCollection);
		String siaDatasetClass = siaDataset.getClass().toString().split(" ")[1];
		String siaCollectionClass = siaCollection.getClass().toString().split(" ")[1];

		// Create the spatiotemporal filters object and then bundle it up so it can be put into hadoop configuration
		SpatiotemporalFilters spatiotemporalFilters = mapreducerConfiguration.buildSpatiotemporalFilters("monthly");
		String spatiotemporalFiltersSerialized = SiaConfigurationUtils.serializeObject(spatiotemporalFilters);

		// Setup the hadoop configuration
		Configuration hadoopConfiguration = getConf();
		SiaConfigurationUtils.addKeysToHadoopConfiguration(mapreducerConfiguration.getBaseConfiguration(), hadoopConfiguration);
		hadoopConfiguration.set(ConfigParameterKeywords.userPropertiesSerialized, userPropertiesSerialized);
		hadoopConfiguration.set(ConfigParameterKeywords.spatiotemporalFiltersSerialized, spatiotemporalFiltersSerialized);
		hadoopConfiguration.set(ConfigParameterKeywords.siaDatasetSerialized, siaDatasetSerialized);
		hadoopConfiguration.set(ConfigParameterKeywords.siaCollectionSerialized, siaCollectionSerialized);
		hadoopConfiguration.set(ConfigParameterKeywords.siaDatasetClass, siaDatasetClass);
		hadoopConfiguration.set(ConfigParameterKeywords.siaCollectionClass, siaCollectionClass);

		Job mapreducerJob = mapreducerConfiguration.createHadoopJob(hadoopConfiguration, userProperties);
		mapreducerJob.setJarByClass(Driver.class);

		// Launch job and wait for completion
		mapreducerJob.waitForCompletion(true);

		return 0;
	}
}