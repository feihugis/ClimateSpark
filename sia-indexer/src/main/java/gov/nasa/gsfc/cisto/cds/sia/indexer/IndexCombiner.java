package gov.nasa.gsfc.cisto.cds.sia.indexer;


import com.google.gson.Gson;
import gov.nasa.gsfc.cisto.cds.sia.core.HibernateUtils;
import gov.nasa.gsfc.cisto.cds.sia.core.collection.SiaCollection;
import gov.nasa.gsfc.cisto.cds.sia.core.common.DAOImpl;
import gov.nasa.gsfc.cisto.cds.sia.core.config.ConfigParameterKeywords;
import gov.nasa.gsfc.cisto.cds.sia.core.config.SiaConfigurationUtils;
import gov.nasa.gsfc.cisto.cds.sia.core.config.UserProperties;
import gov.nasa.gsfc.cisto.cds.sia.core.dataset.SiaDataset;
import gov.nasa.gsfc.cisto.cds.sia.core.variableentities.SiaVariableEntity;
import gov.nasa.gsfc.cisto.cds.sia.core.variableentities.SiaVariableEntityFactory;
import gov.nasa.gsfc.cisto.cds.sia.core.variablemetadata.SiaGenericWritable;
import gov.nasa.gsfc.cisto.cds.sia.core.variablemetadata.SiaVariableAttribute;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mkbowen, Fei Hu on 1/10/17.
 */
public class IndexCombiner extends Reducer<Text, SiaGenericWritable, Text, SiaGenericWritable> {
  private static final Log LOG = LogFactory.getLog(IndexCombiner.class);


  SiaDataset siaDataset;
  SiaCollection siaCollection;
  String whichDataset;
  Session session;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    UserProperties userProperties = (UserProperties) SiaConfigurationUtils.
        deserializeObject(conf.get(ConfigParameterKeywords.userPropertiesSerialized),
                          UserProperties.class);
    whichDataset = userProperties.getDatasetName();
    Gson gson = new Gson();
    String siaDatasetSerialization = conf.get(ConfigParameterKeywords.siaDatasetSerialized);
    String siaCollectionSerialization = conf.get(ConfigParameterKeywords.siaCollectionSerialized);
    System.out.println("In setup");

    try {
      Class<SiaDataset> siaDatasetClass = (Class<SiaDataset>) Class.forName(conf.get(ConfigParameterKeywords.siaDatasetClass));
      siaDataset = gson.fromJson(siaDatasetSerialization, siaDatasetClass);

      Class<SiaCollection> siaCollectionClass = (Class<SiaCollection>) Class.forName(conf.get(ConfigParameterKeywords.siaCollectionClass));
      siaCollection = gson.fromJson(siaCollectionSerialization, siaCollectionClass);
      System.out.println(siaCollection.getValidMin() + " -------");

      /**
      String hibernateConfigSerialization = conf.get(ConfigParameterKeywords.hibernateConfigSerialized);
      ConcurrentHashMap hibernateConfig = gson.fromJson(hibernateConfigSerialization, ConcurrentHashMap.class);


      siaCollection.setTableName(userProperties.getVariableNames()[0].toLowerCase() + "_" + siaCollection.getCollectionName());


      SessionFactory sessionFactory = HibernateUtils.createSessionFactoryWithPhysicalNamingStrategy(hibernateConfig, siaCollection);
**/


      String hibernateConfigXml = siaDataset.getHibernateConfigXmlFile();
      System.out.println("hibernate config xml file: " + hibernateConfigXml);
      siaCollection.setTableName(userProperties.getVariableNames()[0].toLowerCase() + "_" + siaCollection.getCollectionName());
      SessionFactory sessionFactory = HibernateUtils.createSessionFactoryWithPhysicalNamingStrategy(hibernateConfigXml, siaCollection);

      this.session = sessionFactory.openSession();


    } catch(ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public void reduce(Text varName, Iterable<SiaGenericWritable> values,
                     Reducer<Text, SiaGenericWritable, Text, SiaGenericWritable>.Context context)
                                                        throws IOException, InterruptedException {
    DAOImpl dao = new DAOImpl();
    dao.setSession(session);
    List<SiaVariableEntity> siaVariableEntityList = new ArrayList<SiaVariableEntity>();

    for (SiaGenericWritable value : values) {
      SiaVariableAttribute siaVariableAttribute = (SiaVariableAttribute) value.get();
      SiaVariableEntity siaVariableEntity = SiaVariableEntityFactory.getSIAVariableEntity(whichDataset);
      if( siaVariableEntity != null ) {
        siaVariableEntity.initializeEntity(siaVariableAttribute);
        siaVariableEntityList.add(siaVariableEntity);
      } else {
        LOG.error("Cloud not find the SiaVariableEntity for " + whichDataset);
      }
    }

    dao.insertDynamicTableObjectList(siaCollection.getTableName(), siaVariableEntityList);
    session.close();

    IntWritable sum = new IntWritable(siaVariableEntityList.size());
    SiaGenericWritable value = new SiaGenericWritable(sum);

    context.write(new Text(siaCollection.getTableName()), value);
  }
}