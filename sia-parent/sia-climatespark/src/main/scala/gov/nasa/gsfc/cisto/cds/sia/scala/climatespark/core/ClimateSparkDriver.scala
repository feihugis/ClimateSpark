package gov.nasa.gsfc.cisto.cds.sia.scala.climatespark.core

import gov.nasa.gsfc.cisto.cds.sia.core.config.ClimateSparkConfig
import gov.nasa.gsfc.cisto.cds.sia.core.io.key.VarKey
import gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.io.ArraySerializer
import gov.nasa.gsfc.cisto.cds.sia.scala.climatespark.core.io.datastructure.CellOld
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import gov.nasa.gsfc.cisto.cds.sia.scala.climatespark.functions.ClimateRDDFunction._

import scala.annotation.switch

/**
  * Created by Fei Hu on 5/25/17.
  */
class ClimateSparkDriver (sparkContext: SparkContext, climateSparkConfig: ClimateSparkConfig){
  val climateSparkContext = new ClimateSparkContext(sparkContext, climateSparkConfig)

  val climateRDD: RDD[(VarKey, ArraySerializer)] = climateSparkContext.getClimateRDD

  def run(computingType: String): Int = {
    val result = (computingType: @switch) match {
      case "PointTimeSeries" => getPointTimeSeries.collect().length
      case "TimeAvg" => getTimeAvg(climateSparkConfig.getVariable_names.split(",").length).collect().length
      case _ => "Do not support this kind of operations yet"
    }

    if (result != null) 1 else 0
  }

  def getPointTimeSeries: RDD[CellOld] = climateRDD.queryPointTimeSeries

  def getTimeAvg(varNum: Int): RDD[(String, ArraySerializer)] = climateRDD.timeAvg(varNum)
}

object ClimateSparkDriver{

  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf().setMaster("local[4]").setAppName("Test")
    val sc = new SparkContext(sparkConfig)
    val climateSparkConfig = new ClimateSparkConfig("MERRA", "tavg1_2d_int_Nx", "my_job",
      "/Users/feihu/Documents/Data/Merra1.5GB", "/Users/feihu/Desktop/output", "/Users/feihu/Documents/IDEAProjects/sia/sia-parent/sia-core/src/main/resources/merra_entity_map.hbm.xml",
      "CUCNVRN", "hdf", "local",
      "Aavg", "2000", "2016",
      "01", "03", "01", "31",
      "0", "23",
      "0", "100",
      "0", "139")

    val climateSparkDriver = new ClimateSparkDriver(sc, climateSparkConfig)

    print("*******************************  " + climateSparkDriver.run("PointTimeSeries"))
  }
}
