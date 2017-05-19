package gov.nasa.gsfc.cisto.cds.sia.indexer;

import com.google.gson.Gson;
import gov.nasa.gsfc.cisto.cds.sia.core.collection.SiaCollection;
import gov.nasa.gsfc.cisto.cds.sia.core.config.ConfigParameterKeywords;
import gov.nasa.gsfc.cisto.cds.sia.core.config.SiaConfigurationUtils;
import gov.nasa.gsfc.cisto.cds.sia.core.config.UserProperties;
import gov.nasa.gsfc.cisto.cds.sia.core.dataset.SiaDataset;
import gov.nasa.gsfc.cisto.cds.sia.core.variableentities.SiaVariableEntityFactory;
import gov.nasa.gsfc.cisto.cds.sia.core.variablemetadata.SiaGenericWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

/**
 * Created by mkbowen, Fei Hu on 1/10/17.
 */

public class IndexReducerBack extends Reducer<Text, SiaGenericWritable, NullWritable, NullWritable> {

  SiaVariableEntityFactory variableEntityFactory;
  List<String> siaVariableEntityClassNameList;
  SiaDataset siaDataset;
  SiaCollection siaCollection;
  String whichDataset;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    UserProperties userProperties = (UserProperties) SiaConfigurationUtils.deserializeObject(conf.get(ConfigParameterKeywords.userPropertiesSerialized), UserProperties.class);
    whichDataset = userProperties.getDatasetName();
    Gson gson = new Gson();
    String siaDatasetSerialization = conf.get(ConfigParameterKeywords.siaDatasetSerialized);
    String siaCollectionSerialization = conf.get(ConfigParameterKeywords.siaCollectionSerialized);
    Class<SiaDataset> siaDatasetClass;
    Class<SiaCollection> siaCollectionClass;

    // TODO operations for dynamically remapping serialized datasets and collections should be moved out
    try {
      siaDatasetClass = (Class<SiaDataset>) Class.forName(conf.get(ConfigParameterKeywords.siaDatasetClass));
      siaCollectionClass = (Class<SiaCollection>) Class.forName(conf.get(ConfigParameterKeywords.siaCollectionClass));

      siaDataset = gson.fromJson(siaDatasetSerialization, siaDatasetClass);
      siaCollection = gson.fromJson(siaCollectionSerialization, siaCollectionClass);
    } catch(ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public void reduce(Text key,
                     Iterable<SiaGenericWritable> values,
                     Context context) throws IOException, InterruptedException {
    /**
    String currentVariable = key.toString();
    siaCollection.setCurrentVariable(currentVariable);
    String tableName = currentVariable.toLowerCase() + "_" + siaCollection.getCollectionName().toLowerCase();
    siaCollection.setTableName(tableName);
    SiaVariableAttribute siaVariableAttribute;

    String hibernateConfigXml = siaDataset.getHibernateConfigXmlFile();
    SessionFactory sessionFactory = HibernateUtils.createSessionFactoryWithPhysicalNamingStrategy(hibernateConfigXml, siaCollection);
    Session session = sessionFactory.openSession();

    boolean isNewVariableEntity = true;
    String className = "";
    try {
      session.beginTransaction();

      for (SiaGenericWritable value : values) {
        Writable rawValue = value.get();
        siaVariableAttribute = (SiaVariableAttribute) rawValue;

        SiaVariableEntity siaVariableEntity = variableEntityFactory.getSIAVariableEntity(whichDataset);
        siaVariableEntity.initializeEntity(siaVariableAttribute);
        if (isNewVariableEntity) {
          className = siaVariableEntity.getClass().toString().split(" ")[1];
          isNewVariableEntity = false;
        }

        session.save(siaVariableEntity);
      }
      session.getTransaction().commit();
      session.close();
    } catch (Exception e) {
      session.getTransaction().rollback();
    }
     **/
  }
}