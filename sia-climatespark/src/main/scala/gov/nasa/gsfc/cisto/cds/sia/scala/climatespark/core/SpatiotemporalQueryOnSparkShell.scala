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
  * Created by Fei Hu on 10/28/17.
  */
object SpatiotemporalQueryOnSparkShell {

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  def query(sc: SparkContext, args: Array[String]) {
    val hadoopConf = new HadoopConfiguration(args).getHadoopConf
    val climateSparkContext = new ClimateSparkContext(sc, hadoopConf)
    val sqlContext = new SQLContext(climateSparkContext.getSparkContext)
    import sqlContext.implicits._

    val climateRDD = climateSparkContext.getClimateRDD

    val monthlyAvg = climateRDD.monthlyAvg(1).cache()

    val df = monthlyAvg.toDF("VarName", "Time", "Avg")
    df.show()

    val monthlyMeanArray = monthlyAvg.map(tuple => tuple._3).collect()

    println("Monthly Avg: ", monthlyMeanArray.sum/monthlyMeanArray.size)

  }
}
