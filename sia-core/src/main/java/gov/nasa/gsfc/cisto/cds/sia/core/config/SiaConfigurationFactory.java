package gov.nasa.gsfc.cisto.cds.sia.core.config;

/**
 * The type Sia configuration factory.
 */
public class SiaConfigurationFactory {

    /**
     * Gets sia configuration.
     *
     * @param jobType the job type
     * @return the sia configuration
     */
    public static SiaConfiguration getSiaConfiguration(String jobType) {
        if(jobType.equalsIgnoreCase("preprocessor")) {
            return new PreprocessorConfiguration();
        }
        else if(jobType.equalsIgnoreCase("indexer")) {
            return new IndexerConfiguration();
        }
        else if(jobType.equalsIgnoreCase("mapreducer")) {
            return new MapreducerConfiguration();
        }

        return null;
    }
}
