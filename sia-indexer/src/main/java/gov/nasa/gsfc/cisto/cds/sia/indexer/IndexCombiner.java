package gov.nasa.gsfc.cisto.cds.sia.indexer;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.hibernate.Session;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import gov.nasa.gsfc.cisto.cds.sia.core.config.ConfigParameterKeywords;
import gov.nasa.gsfc.cisto.cds.sia.core.variableentities.SiaVariableEntity;
import gov.nasa.gsfc.cisto.cds.sia.core.variableentities.SiaVariableEntityFactory;
import gov.nasa.gsfc.cisto.cds.sia.core.variablemetadata.SiaGenericWritable;
import gov.nasa.gsfc.cisto.cds.sia.core.variablemetadata.SiaVariableAttribute;
import gov.nasa.gsfc.cisto.cds.sia.hibernate.DAOImpl;
import gov.nasa.gsfc.cisto.cds.sia.hibernate.HibernateUtil;
import gov.nasa.gsfc.cisto.cds.sia.hibernate.PhysicalNameStrategyImpl;

/**
 * Created by Fei Hu on 1/10/17.
 */
public class IndexCombiner extends Reducer<Text, SiaGenericWritable, Text, SiaGenericWritable> {
  private static final Log LOG = LogFactory.getLog(IndexCombiner.class);
  private String whichDataset;
  private String whichCollection;
  private Configuration conf;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    conf = context.getConfiguration();
    whichDataset = conf.get(ConfigParameterKeywords.DATASET_NAME);
    whichCollection = conf.get(ConfigParameterKeywords.COLLECTION_NAME);
  }

  public void reduce(Text varName, Iterable<SiaGenericWritable> values,
                     Reducer<Text, SiaGenericWritable, Text, SiaGenericWritable>.Context context)
                                                        throws IOException, InterruptedException {

    String tableName = String.format("%s_%s", varName.toString().toLowerCase(),
                                     whichCollection.toLowerCase());

    Class entityClass = SiaVariableEntityFactory.getSIAVariableEntity(whichDataset).getClass();

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

    PhysicalNameStrategyImpl physicalNameStrategy = new PhysicalNameStrategyImpl(tableName);
    HibernateUtil hibernateUtil = new HibernateUtil();

    hibernateUtil.createSessionFactoryWithPhysicalNamingStrategy(conf,
                                                                 physicalNameStrategy,
                                                                 entityClass);
    Session session = hibernateUtil.getSession();
    DAOImpl<SiaVariableEntity> dao = new DAOImpl<SiaVariableEntity>();
    dao.setSession(session);

    dao.insertDynamicTableObjectList(tableName, siaVariableEntityList.listIterator());
    hibernateUtil.closeSession();
    hibernateUtil.closeSessionFactory();

    IntWritable sum = new IntWritable(siaVariableEntityList.size());
    SiaGenericWritable value = new SiaGenericWritable(sum);

    context.write(new Text(tableName), value);
  }
}