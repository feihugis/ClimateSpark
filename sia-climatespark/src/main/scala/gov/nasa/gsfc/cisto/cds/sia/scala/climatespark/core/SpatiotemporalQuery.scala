package gov.nasa.gsfc.cisto.cds.sia.scala.climatespark.core

import gov.nasa.gsfc.cisto.cds.sia.core.config.HadoopConfiguration
import gov.nasa.gsfc.cisto.cds.sia.scala.climatespark.functions.ClimateRDDFunction._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import gov.nasa.gsfc.cisto.cds.sia.scala.climatespark.core.ClimateSparkContext
import gov.nasa.gsfc.cisto.cds.sia.scala.climatespark.core.io.datastructure.{Cell3D, Cell4D, Cell5D}
import gov.nasa.gsfc.cisto.cds.sia.scala.climatespark.core.io.datastructure.CellFactory._
import org.apache.spark.rdd.RDD
import ucar.nc2.dataset.NetcdfDataset

/**
  * Created by Fei Hu on 12/22/16.
  */
object SpatiotemporalQuery {

  def main(args: Array[String]) {
    ///var/lib/hadoop-hdfs/0326/properties/sia_analytics.properties
    val input = Array[String]("/Users/feihu/Documents/IDEAProjects/ClimateSpark/properties/sia_analytics.properties")
    // val input = Array[String]("/home/u17/indexing/ClimateSpark/properties/sia_analytics.properties")
    val hadoopConf = new HadoopConfiguration(input).getHadoopConf
    //val sc = new SparkContext()

    //val sc = new ClimateSparkContext(configFile, "local[6]", "test")
    val climateSparkContext = new ClimateSparkContext(hadoopConf, "local[6]", "test")
    //val climateSparkContext = new ClimateSparkContext(sc, hadoopConf)
    val sqlContext = new SQLContext(climateSparkContext.getSparkContext)
    import sqlContext.implicits._

    val climateRDD = climateSparkContext.getClimateRDD

    val monthlyAvg = climateRDD.monthlyAvg(1).toDF("VarName", "Time", "Avg")
    monthlyAvg.show()


    val cellRDD:RDD[Cell4D] = climateRDD.getCells.map(cell => cell.asInstanceOf[Cell4D])

    val df = sqlContext.createDataFrame(cellRDD)
    df.registerTempTable("merra")
    sqlContext.sql("SELECT d0 AS D0, d1 AS D1, d2 AS D2, d3 AS D3, value AS Value FROM merra").show()
  }
}
