package gov.nasa.gsfc.cisto.cds.sia.scala.climatespark.core.rdd

import gov.nasa.gsfc.cisto.cds.sia.core.io.key.VarKey
import gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.inputformat.SiaInputFormat
import gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.io.ArraySerializer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.NewHadoopRDD

class ClimateRDD(
  sc : SparkContext,
  @transient conf: Configuration) extends NewHadoopRDD(sc,
    classOf[SiaInputFormat]
      .asInstanceOf[Class[F] forSome {type F <: InputFormat[VarKey, ArraySerializer]}],
    classOf[gov.nasa.gsfc.cisto.cds.sia.core.io.key.VarKey],
    classOf[gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.io.ArraySerializer],
    conf){
  }
