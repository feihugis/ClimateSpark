package gov.nasa.gsfc.cisto.cds.sia.core.config;

import gov.nasa.gsfc.cisto.cds.sia.core.collection.SiaCollection;
import gov.nasa.gsfc.cisto.cds.sia.core.dataset.SiaDataset;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * The interface Sia configuration.
 */
public interface SiaConfiguration {

    /**
     * Build base configuration.
     *
     * @param args the args
     * @throws ConfigurationException the configuration exception
     */
    void buildBaseConfiguration(String args[]) throws ConfigurationException;

    /**
     * Gets base configuration.
     *
     * @return the base configuration
     */
    CompositeConfiguration getBaseConfiguration();

    /**
     * Build user properties user properties.
     *
     * @return the user properties
     */
    UserProperties buildUserProperties();

    /**
     * Build spatiotemporal filters spatiotemporal filters.
     *
     * @param dateType the date type
     * @return the spatiotemporal filters
     */
    SpatiotemporalFilters buildSpatiotemporalFilters(String dateType);

    /**
     * Build sia dataset sia dataset.
     *
     * @param userProperties the user properties
     * @return the sia dataset
     */
// TODO Eventually these two should also include the metadata from psql, for now they pull from xml
    SiaDataset buildSIADataset(UserProperties userProperties);

    /**
     * Build sia collection sia collection.
     *
     * @param userProperties the user properties
     * @return the sia collection
     */
    SiaCollection buildSIACollection(UserProperties userProperties);

    /**
     * Create hadoop job job.
     *
     * @param hadoopConfiguration the hadoop configuration
     * @param userProperties      the user properties
     * @return the job
     * @throws Exception the exception
     */
    Job createHadoopJob(Configuration hadoopConfiguration, UserProperties userProperties) throws Exception;
}
