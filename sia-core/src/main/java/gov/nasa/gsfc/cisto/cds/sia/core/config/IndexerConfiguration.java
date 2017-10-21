package gov.nasa.gsfc.cisto.cds.sia.core.config;

import gov.nasa.gsfc.cisto.cds.sia.core.collection.SiaCollection;
import gov.nasa.gsfc.cisto.cds.sia.core.collection.SiaCollectionFactory;
import gov.nasa.gsfc.cisto.cds.sia.core.dataset.SiaDataset;
import gov.nasa.gsfc.cisto.cds.sia.core.dataset.SiaDatasetFactory;
import gov.nasa.gsfc.cisto.cds.sia.core.variablemetadata.SiaGenericWritable;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stream.StreamSource;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

/**
 * The type Indexer configuration.
 */
public class IndexerConfiguration extends CompositeConfiguration implements SiaConfiguration {

    private CompositeConfiguration baseConfig;
    private boolean isBuilt = false;
    private static final Log LOG = LogFactory.getLog(IndexerConfiguration.class);
    private String jobType = "indexer";

    public void buildBaseConfiguration(String[] args) throws ConfigurationException {
        if (isBuilt) {
            return;
        }

        validateCommandLineInput(args);
        this.baseConfig = new CompositeConfiguration();
        File propertiesFile = new File(args[0]);

        if(propertiesFile.canRead()) {
            baseConfig.addConfiguration(new PropertiesConfiguration(propertiesFile));
            LOG.info("Indexer properties file successfully loaded from " + propertiesFile.getPath());
        } else {
            LOG.error("Could not find indexer properties file in " + propertiesFile.getPath());
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

        String[] variableNames = convertVariableNamesToArray(baseConfig.getString("variable.names"));

        UserProperties userProperties = new UserProperties.UserPropertiesBuilder(datasetName, jobName)
                .collectionName(baseConfig.getString("collection.name"))
                .inputPath(baseConfig.getString("input.path"))
                .outputPath(baseConfig.getString("output.path"))
                .fileExtension(baseConfig.getString("file.extension"))
                .variableNames(variableNames)
                .mapreduceFrameworkName(baseConfig.getString("mapreduce.framework.name"))
                .filesPerMapTask(baseConfig.getInt("files.per.map.task"))
                .build();

        return userProperties;
    }

  public SpatiotemporalFilters buildSpatiotemporalFilters(String dateType) {
    throw new UnsupportedOperationException("Indexer does not support spatiotemporal filters.");
  }

    public SiaDataset buildSIADataset(UserProperties userProperties) {
        validateBaseConfigIsBuilt();

        SiaDatasetFactory siaDatasetFactory = new SiaDatasetFactory();
        SiaDataset siaDataset = siaDatasetFactory.getSIADataset(userProperties.getDatasetName());

        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(siaDataset.getClass());
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            InputStream siaDatasetInputStream = getClass().getResourceAsStream(siaDataset.getXmlDatasetSetupFile());
            siaDataset = (SiaDataset) unmarshaller.unmarshal(siaDatasetInputStream);
        }
        catch (JAXBException jex) {
            System.err.println(jex);
        }

        return siaDataset;
    }

    public SiaCollection buildSIACollection(UserProperties userProperties) {
        validateBaseConfigIsBuilt();

        SiaCollectionFactory siaCollectionFactory = new SiaCollectionFactory();
        SiaCollection siaCollection = siaCollectionFactory.getSIACollection(userProperties.getDatasetName());

        try {
            XMLInputFactory xmlInputFactory = XMLInputFactory.newFactory();
            StreamSource streamSource = new StreamSource(getClass().getResourceAsStream(siaCollection.getXmlCollectionSetupFile()));
            XMLStreamReader xmlStreamReader = xmlInputFactory.createXMLStreamReader(streamSource);

            while(xmlStreamReader.hasNext()) {
                if (xmlStreamReader.isStartElement() && xmlStreamReader.getLocalName().equals(userProperties.getCollectionName())) {
                    break;
                }
                xmlStreamReader.next();
            }

            try {
                JAXBContext jaxbContext = JAXBContext.newInstance(siaCollection.getClass());
                Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();

                JAXBElement<SiaCollection> jaxbElement = (JAXBElement<SiaCollection>) unmarshaller.unmarshal(xmlStreamReader, siaCollection.getClass());
                siaCollection = jaxbElement.getValue();
                siaCollection.setXmlHibernateTableMappingFilePath(userProperties.getXmlHibernateTableMappingFilePath());
            }
            catch (JAXBException jex) {
                jex.printStackTrace();
            }
        }
        catch (XMLStreamException xex) {
            xex.printStackTrace();
        }

        return siaCollection;
    }

    public Job createHadoopJob(Configuration hadoopConfiguration, UserProperties userProperties) throws Exception {
        validateBaseConfigIsBuilt();

        Job job = null;
        try {
            job = Job.getInstance(hadoopConfiguration);

            String mapperClass = "gov.nasa.gsfc.cisto.cds.sia.indexer.IndexMapper";
            Mapper<?, ?, ?, ?> mapper = (Mapper<?, ?, ?, ?>) GeneralClassLoader.loadObject(mapperClass, null, null);
            job.setMapperClass(mapper.getClass());
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(SiaGenericWritable.class);

            String combinerClass = "gov.nasa.gsfc.cisto.cds.sia.indexer.IndexCombiner";
            Reducer<?,?,?,?> combiner = (Reducer<?, ?, ?, ?>) GeneralClassLoader.loadObject(combinerClass, null, null);
            job.setCombinerClass(combiner.getClass());

            String reducerClass = "gov.nasa.gsfc.cisto.cds.sia.indexer.IndexReducer";
            Reducer<?, ?, ?, ?> reducer = (Reducer<?, ?, ?, ?>) GeneralClassLoader.loadObject(reducerClass, null, null);
            job.setReducerClass(reducer.getClass());
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            String inputFormatClass = "gov.nasa.gsfc.cisto.cds.sia.indexer.IndexInputFormat";
            FileInputFormat fileInputFormat = (FileInputFormat) GeneralClassLoader.loadObject(inputFormatClass, null, null);
            job.setInputFormatClass(fileInputFormat.getClass());

            fileInputFormat.setInputDirRecursive(job, true);
            fileInputFormat.addInputPath(job, new Path(userProperties.getInputPath()));

            FileOutputFormat.setOutputPath(job, new Path(userProperties.getOutputPath() + new Date().getTime()));

        } catch (IOException e) {
            e.printStackTrace();
        }

        return job;
    }

    private String[] convertVariableNamesToArray(String variableNames) {
        String[] variableNamesArray = variableNames.split(",");
        return variableNamesArray;
    }

    private void validateBaseConfigIsBuilt() {
        if (!isBuilt) {
            LOG.error("Base configuration not built yet.");
            System.exit(2);
        }
    }

    private void validateCommandLineInput(String args[]) {
        if(args.length != 1) {
            System.err.println("Usage: <sia-indexer-properties-file-path");
            LOG.error("Usage: <sia-indexer-properties-file-path");
            System.exit(2);
        }
    }
}