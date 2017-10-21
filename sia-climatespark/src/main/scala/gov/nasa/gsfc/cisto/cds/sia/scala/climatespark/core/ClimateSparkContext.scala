package gov.nasa.gsfc.cisto.cds.sia.scala.climatespark.core

import gov.nasa.gsfc.cisto.cds.sia.core.config.{ClimateSparkConfig, HadoopConfiguration}
import gov.nasa.gsfc.cisto.cds.sia.core.io.key.VarKey
import gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.inputformat.SiaInputFormat
import gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.io.ArraySerializer
import gov.nasa.gsfc.cisto.cds.sia.scala.climatespark.core.io.ClimateSparkKryoRegistrator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Fei Hu on 12/15/16.
  */
class ClimateSparkContext (@transient val sparkContext: SparkContext){
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  val hConf = new Configuration()
  val dataChunk = classOf[gov.nasa.gsfc.cisto.cds.sia.core.io.key.VarKey]
  val arraySerializer = classOf[gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.io.ArraySerializer]
  val inputFormat = classOf[SiaInputFormat].asInstanceOf[Class[F] forSome {type F <: InputFormat[VarKey, ArraySerializer]}]


  def this(conf: SparkConf) {
    this(new SparkContext(conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[ClimateSparkKryoRegistrator].getName)))
  }

  def this(sc: SparkContext, hadoopConf: String) {
    this(sc)
    this.hConf.addResource(new Path(hadoopConf))
    this.sparkContext.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[ClimateSparkKryoRegistrator].getName)
  }

  def this(sparkConf: SparkConf, hadoopConf: String) {
    this(new SparkContext(sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[ClimateSparkKryoRegistrator].getName)))
    this.hConf.addResource(new Path(hadoopConf))
  }

  def this(hadoopConf: String, uri: String, appName: String) {
    this(new SparkContext(new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[ClimateSparkKryoRegistrator].getName)
      .setMaster(uri)
      .setAppName(appName)))
    this.hConf.addResource(new Path(hadoopConf))
  }


  def this(hadoopConf: Configuration, uri: String, appName: String) {
    this(new SparkContext(new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[ClimateSparkKryoRegistrator].getName)
      .setMaster(uri)
      .setAppName(appName)))
    this.hConf.addResource(hadoopConf)
  }

  def this(sc: SparkContext, hadoopConf: Configuration, uri: String, appName: String) {
    this(sc)
    this.sparkContext.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[ClimateSparkKryoRegistrator].getName)

    this.hConf.addResource(hadoopConf)
  }

  def this(sc: SparkContext, hadoopConf: Configuration) {
    this(sc)
    this.sparkContext.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[ClimateSparkKryoRegistrator].getName)

    this.hConf.addResource(hadoopConf)
  }

  def this(sc: SparkContext, climateSparkConfig: ClimateSparkConfig) {
    this(sc)
    this.sparkContext.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[ClimateSparkKryoRegistrator].getName)
    val hadoopConf = new HadoopConfiguration(climateSparkConfig).getHadoopConf
    this.hConf.addResource(hadoopConf)
  }

  def getClimateRDD: RDD[(VarKey, ArraySerializer)] = {
    this.sparkContext.newAPIHadoopRDD(this.hConf, inputFormat, dataChunk, arraySerializer).map(rdd => rdd ).filter(_._1 != null)
  }

  def getHadoopConfig = this.hConf

  def getSparkContext = this.sparkContext
}
