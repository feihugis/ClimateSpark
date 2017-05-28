package gov.nasa.gsfc.cisto.cds.sia.scala.climatespark.core

import gov.nasa.gsfc.cisto.cds.sia.core.config.HadoopConfiguration
import gov.nasa.gsfc.cisto.cds.sia.scala.climatespark.functions.ClimateRDDFunction._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import gov.nasa.gsfc.cisto.cds.sia.scala.climatespark.core.ClimateSparkContext
import ucar.nc2.dataset.NetcdfDataset


/**
  * Created by Fei Hu on 12/22/16.
  */
object SpatiotemporalQuery {

  def main(args: Array[String]) {
    val configFile = ""
    ///var/lib/hadoop-hdfs/0326/properties/sia_analytics.properties
    val input = Array[String]("/Users/feihu/Documents/IDEAProjects/sia/sia-parent/properties/sia_analytics.properties")
    val hadoopConf = new HadoopConfiguration(input).getHadoopConf
    //val sc = new SparkContext()

    //val sc = new ClimateSparkContext(configFile, "local[6]", "test")
    val sc = new ClimateSparkContext(hadoopConf, "local[6]", "test")
    //val climateSparkContext = new ClimateSparkContext(sc, hadoopConf)
    val sqlContext = new SQLContext(sc.getSparkContext)

    val climateRDD = sc.getClimateRDD
    val cellRDD = climateRDD.queryPointTimeSeries
    val df = sqlContext.createDataFrame(cellRDD)

    df.registerTempTable("merra")
    sqlContext.sql("Select * From merra").show()

  }
}
