package gov.nasa.gsfc.cisto.cds.sia.indexer;

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
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringReader;
import java.io.StringWriter;

/**
 * Main entry point for the MapReduce job which builds the index
 *
 * @author mkbowen
 *
 */
public class IndexDriver extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(IndexDriver.class);

  /**
   * Main driver for Index Building
   *
   * @param args
   * @throws Exception
     */
  public static void main(String[] args) throws Exception {

    String classpath = System.getProperty("java.class.path");
      LOG.debug(">> CLASSPATH: \n" + classpath);

      int exitCode = ToolRunner.run(new Configuration(), new IndexDriver(), args);
      System.exit(exitCode);
  }

  /**
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   *
   * @param args
   * @return
   * @throws Exception
     */
  public int run(String args[]) throws Exception {
    System.out.println("Java runtime version: " + System.getProperty("java.version"));
    String jobType = "indexer";
    SiaConfigurationFactory siaConfigurationFactory = new SiaConfigurationFactory();
    SiaConfiguration indexerConfiguration = siaConfigurationFactory.getSiaConfiguration(jobType);
    indexerConfiguration.buildBaseConfiguration(args);

    // Create the user properties object and then bundle it up so it can be put into hadoop configuration
    UserProperties userProperties = indexerConfiguration.buildUserProperties();
    //String userPropertiesSerialized = SiaConfigurationUtils.serializeObject(userProperties);


    // Setup the hadoop configuration
    Configuration hadoopConfiguration = getConf();

    //Add all configurations to Hadoop configurations
    SiaConfigurationUtils.addKeysToHadoopConfiguration(indexerConfiguration.getBaseConfiguration(), hadoopConfiguration);
    //hadoopConfiguration.set(ConfigParameterKeywords.userPropertiesSerialized, userPropertiesSerialized);

    Job indexerJob = indexerConfiguration.createHadoopJob(hadoopConfiguration, userProperties);
    indexerJob.setJarByClass(IndexDriver.class);

    // Launch job and wait for completion
    indexerJob.waitForCompletion(true);

    return 0;
  }

  private static String xmlToString(Document doc) {
    TransformerFactory transformerFactory = TransformerFactory.newInstance();
    Transformer transformer;
    try {
      transformer = transformerFactory.newTransformer();
      StringWriter stringWriter = new StringWriter();
      transformer.transform(new DOMSource(doc), new StreamResult(stringWriter));
      String output = stringWriter.getBuffer().toString();
      return output;
    } catch (TransformerException e) {
      e.printStackTrace();
    }

    return null;
  }

  private static Document stringToXml(String xmlString) {
    DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder documentBuilder;

    try {
      documentBuilder = documentBuilderFactory.newDocumentBuilder();
      Document document = documentBuilder.parse(new InputSource(new StringReader(xmlString)));
      return document;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }
}