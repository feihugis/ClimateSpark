package gov.nasa.gsfc.cisto.cds.sia.scala.climatespark.core.io

import com.esotericsoftware.kryo.Kryo
import gov.nasa.gsfc.cisto.cds.sia.core.io.SiaChunk
import gov.nasa.gsfc.cisto.cds.sia.core.io.key.VarKey
import gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.io.{ArrayFloatSerializer, ArraySerializer}
import org.apache.spark.serializer.KryoRegistrator

/**
  * Created by Fei Hu on 12/16/16.
  */
class ClimateSparkKryoRegistrator extends KryoRegistrator{
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[ArrayFloatSerializer])
    kryo.register(classOf[ArraySerializer])
    kryo.register(classOf[SiaChunk])
    kryo.register(classOf[ArraySerializer])
    kryo.register(classOf[VarKey])
    kryo.register(classOf[scala.Tuple2[SiaChunk, ArrayFloatSerializer]])
    kryo.register(classOf[scala.Tuple2[VarKey, ArrayFloatSerializer]])
  }
}
