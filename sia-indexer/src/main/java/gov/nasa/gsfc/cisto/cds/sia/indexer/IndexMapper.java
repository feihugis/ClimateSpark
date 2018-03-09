package gov.nasa.gsfc.cisto.cds.sia.indexer;

import gov.nasa.gsfc.cisto.cds.sia.core.config.ConfigParameterKeywords;
import gov.nasa.gsfc.cisto.cds.sia.core.config.SiaConfigurationUtils;
import gov.nasa.gsfc.cisto.cds.sia.core.config.UserProperties;
import gov.nasa.gsfc.cisto.cds.sia.core.variablemetadata.SiaGenericWritable;
import gov.nasa.gsfc.cisto.cds.sia.core.variablemetadata.SiaVariableAttribute;
import gov.nasa.gsfc.cisto.cds.sia.core.variablemetadata.SiaVariableMetadataFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
//import gov.nasa.gsfc.cisto.cds.sia.core.VariableManager;
//import gov.nasa.gsfc.cisto.cds.sia.core.VariableManagerFactory;


/**
 * Mapper for building index
 *
 * @author mkbowen, feihu
 *
 */
public class IndexMapper extends Mapper<Text, Text, Text, SiaGenericWritable> {

  SiaVariableMetadataFactory variableMetadataFactory;
  private static final Log LOG = LogFactory.getLog(IndexMapper.class);

  /**
   *
   * @param buildIndexKeyFile file name being processed by this mapper
   * @param buildIndexValueVariables variables of interest in the file
   * @param context job information
   * @throws IOException
   * @throws InterruptedException
     */
  @Override
  public void map(Text buildIndexKeyFile,
                  Text buildIndexValueVariables,
                  Context context) throws IOException, InterruptedException {

    String file = buildIndexKeyFile.toString();
    String[] variables = buildIndexValueVariables.toString().split(",");
    Configuration conf = context.getConfiguration();
    FileSystem fileSystem = FileSystem.get(conf);

    String whichVariableMetadata = conf.get(ConfigParameterKeywords.DATASET_NAME);

    variableMetadataFactory = new SiaVariableMetadataFactory();
    SiaVariableAttribute variableMetadata = variableMetadataFactory.getVariableMetadata(whichVariableMetadata);
    List<SiaVariableAttribute> siaVariableAttributeList = variableMetadata.getVariableMetadataList(fileSystem, file, variables);

    //write to the reducer
    for(SiaVariableAttribute siaVariableAttribute : siaVariableAttributeList){
      Text varName = new Text(siaVariableAttribute.getVariableName());
      context.write(varName, new SiaGenericWritable(siaVariableAttribute));
    }

  }
}