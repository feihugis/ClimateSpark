package gov.nasa.gsfc.cisto.cds.sia.core.config;

import gov.nasa.gsfc.cisto.cds.sia.core.collection.SiaCollection;
import gov.nasa.gsfc.cisto.cds.sia.core.collection.SiaCollectionFactory;
import gov.nasa.gsfc.cisto.cds.sia.core.dataset.SiaDataset;
import gov.nasa.gsfc.cisto.cds.sia.core.dataset.SiaDatasetFactory;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.util.Date;

/**
 * The type Mapreducer configuration.
 */
public class MapreducerConfiguration extends CompositeConfiguration implements SiaConfiguration {

    private CompositeConfiguration baseConfig;
    private boolean isBuilt = false;
    private static final Log LOG = LogFactory.getLog(MapreducerConfiguration.class);
    private String jobType = "mapreducer";

    public void buildBaseConfiguration(ClimateSparkConfig climateSparkConfig) {
      this.baseConfig = new CompositeConfiguration();
      baseConfig.setProperty("dataset.name", climateSparkConfig.getDataset_name());
      baseConfig.setProperty("job.name", climateSparkConfig.getJob_name());
      baseConfig.setProperty("variable.names", climateSparkConfig.getVariable_names());
      baseConfig.setProperty("collection.name", climateSparkConfig.getCollection_name());
      baseConfig.setProperty("input.path", climateSparkConfig.getInput_path());
      baseConfig.setProperty("output.path", climateSparkConfig.getOutput_path());
      baseConfig.setProperty("file.extension", climateSparkConfig.getFile_extension());
      baseConfig.setProperty("mapreduce.framework.name", climateSparkConfig.getMapreduce_framework_name());
      baseConfig.setProperty("threads.per.node", climateSparkConfig.getThreads_per_node());
      baseConfig.setProperty("analytics.operation", climateSparkConfig.getAnalytics_operation());
      baseConfig.setProperty("number.reducers", climateSparkConfig.getNumber_reducers());

      baseConfig.setProperty("year.start", climateSparkConfig.getYear_start());

      baseConfig.setProperty("month.start", climateSparkConfig.getMonth_start());
      baseConfig.setProperty("day.start", climateSparkConfig.getDay_start());
      baseConfig.setProperty("hour.start", climateSparkConfig.getHour_start());
      baseConfig.setProperty("height.start", climateSparkConfig.getHeight_start());
      baseConfig.setProperty("lat.start", climateSparkConfig.getLat_start());
      baseConfig.setProperty("lon.start", climateSparkConfig.getLon_start());
      baseConfig.setProperty("year.end", climateSparkConfig.getYear_end());
      baseConfig.setProperty("month.end", climateSparkConfig.getMonth_end());
      baseConfig.setProperty("day.end", climateSparkConfig.getDay_end());
      baseConfig.setProperty("hour.end", climateSparkConfig.getHour_end());
      baseConfig.setProperty("height.end", climateSparkConfig.getHeight_end());
      baseConfig.setProperty("lat.end", climateSparkConfig.getLat_end());
      baseConfig.setProperty("lon.end", climateSparkConfig.getLon_end());
      baseConfig.setProperty("xml.hibernate.table.mapping.file.path", climateSparkConfig.getXml_hibernate_table_mapping_file_path());

      SiaConfigurationUtils.validateKeys(baseConfig, jobType);
      isBuilt = true;
    }

    public void buildBaseConfiguration(String[] args) throws ConfigurationException {
        if(isBuilt) {
            return;
        }

        validateCommandLineInput(args);
        this.baseConfig = new CompositeConfiguration();
        File propertiesFile = new File(args[0]);



        if(propertiesFile.canRead()) {
            baseConfig.addConfiguration(new PropertiesConfiguration(propertiesFile));
            LOG.info("Mapreducer properties file successfully loaded from " + propertiesFile.getPath());
        } else {
            LOG.error("Could not find mapreducer properties file in " + propertiesFile.getPath());
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
                .threadsPerNode(baseConfig.getInt("threads.per.node"))
                .analyticsOperation(baseConfig.getString("analytics.operation"))
                .numberReducers(baseConfig.getInt("number.reducers"))
                .build();

        return userProperties;
    }

    public SpatiotemporalFilters buildSpatiotemporalFilters(String dateType) {
          validateBaseConfigIsBuilt();
          SpatiotemporalFilters.SpatiotemporalFiltersBuilder spatiotemporalFiltersBuilder = new SpatiotemporalFilters.SpatiotemporalFiltersBuilder();

          /*if (dateType.toLowerCase().equals("monthly")) {

          spatiotemporalFiltersBuilder.buildMonthlyFilter(baseConfig.getString("year.start"),
                                  baseConfig.getString("month.start"),
                                  baseConfig.getString("year.end"),
                                  baseConfig.getString("month.end"));

            //TODO fix the dateType word for other temporal types
          } else if (dateType.toLowerCase().equals("hourly")) {
            spatiotemporalFiltersBuilder.buildDailyFilter(baseConfig.getString("year.start"),
                                baseConfig.getString("month.start"),
                                baseConfig.getString("day.start"),
                                baseConfig.getString("year.end"),
                                baseConfig.getString("month.end"),
                                baseConfig.getString("day.end"));
          }*/

      spatiotemporalFiltersBuilder.buildDailyFilter(baseConfig.getString("year.start"),
                                                    baseConfig.getString("month.start"),
                                                    baseConfig.getString("day.start"),
                                                    baseConfig.getString("hour.start"),
                                                    baseConfig.getString("height.start"),
                                                    baseConfig.getString("lat.start"),
                                                    baseConfig.getString("lon.start"),
                                                    baseConfig.getString("year.end"),
                                                    baseConfig.getString("month.end"),
                                                    baseConfig.getString("day.end"),
                                                    baseConfig.getString("hour.start"),
                                                    baseConfig.getString("height.start"),
                                                    baseConfig.getString("lat.start"),
                                                    baseConfig.getString("lon.start"));

          SpatiotemporalFilters spatiotemporalFilters = spatiotemporalFiltersBuilder
            .hourStart(baseConfig.getString("hour.start"))
            .hourEnd(baseConfig.getString("hour.end"))
            .heightStart(baseConfig.getString("height.start"))
            .heightEnd(baseConfig.getString("height.end"))
            .latStart(baseConfig.getString("lat.start"))
            .latEnd(baseConfig.getString("lat.end"))
            .lonStart(baseConfig.getString("lon.start"))
            .lonEnd(baseConfig.getString("lon.end"))
            .build();

           return spatiotemporalFilters;
    }


    /**
     * Build spatiotemporal filters spatiotemporal filters.
     *
     * @return the spatiotemporal filters
     */
    public SpatiotemporalFilters buildSpatiotemporalFilters() {
    return buildSpatiotemporalFilters("daily");
  }


    public SiaDataset buildSIADataset(UserProperties userProperties) {
        validateBaseConfigIsBuilt();

        SiaDatasetFactory siaDatasetFactory = new SiaDatasetFactory();
        SiaDataset siaDataset = siaDatasetFactory.getSIADataset(userProperties.getDatasetName());

        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(siaDataset.getClass());
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();

            siaDataset = (SiaDataset) unmarshaller.unmarshal(getClass().getResourceAsStream(siaDataset.getXmlDatasetSetupFile()));
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
                System.err.println(jex);
            }
        }
        catch (XMLStreamException xex) {
            System.err.println(xex);
        }

        return siaCollection;
    }

    public Job createHadoopJob(Configuration hadoopConfiguration, UserProperties userProperties) throws Exception {
        validateBaseConfigIsBuilt();

        Job job = null;
        try {
            job = Job.getInstance(hadoopConfiguration);

            String mapperClass = "gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.map." + userProperties.getAnalyticsOperation() + "Mapper";
            Mapper<?, ?, ?, ?> mapper = (Mapper<?, ?, ?, ?>) GeneralClassLoader.loadObject(mapperClass, null, null);
            job.setMapperClass(mapper.getClass());
            job.setMapOutputKeyClass(Text.class);

            // TODO remove hard coding for output class
            String outputValueClass = "gov.nasa.gsfc.cisto.cds.sia.core.io.value.AreaAvgWritable";
            Object outputValue = GeneralClassLoader.loadObject(outputValueClass, null, null);
            job.setMapOutputValueClass(outputValue.getClass());

            String reducerClass = "gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.reduce." + userProperties.getAnalyticsOperation() + "Reducer";
            Reducer<?, ?, ?, ?> reducer = (Reducer<?, ?, ?, ?>) GeneralClassLoader.loadObject(reducerClass, null, null);
            job.setReducerClass(reducer.getClass());

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(outputValue.getClass());
            job.setNumReduceTasks(userProperties.getNumberReducers());

            // TODO add logic for combiner

            String inputFormatClass = "gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.inputformat.SiaInputFormat";
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
            System.err.println("Usage: <sia-mapreducer-properties-file-path");
            LOG.error("Usage: <sia-mapreducer-properties-file-path");
            System.exit(2);
        }
    }
}
