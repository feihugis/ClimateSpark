package gov.nasa.gsfc.cisto.cds.sia.scala.climatespark.functions

import gov.nasa.gsfc.cisto.cds.sia.core.io.key.VarKey
import gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.io.ArraySerializer
import gov.nasa.gsfc.cisto.cds.sia.scala.climatespark.core.io.datastructure.CellOld
import org.apache.spark.rdd.RDD
import ucar.ma2.MAMath

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Fei Hu on 12/22/16.
  */
class ClimateRDDFunction (self: RDD[(VarKey, ArraySerializer)]) extends Serializable{
  val MISSING_VALUE = 9.9999999E14f


  def queryPointTimeSeries: RDD[CellOld] = {
    self.flatMap(tuple => {
      val dataChunk = tuple._1
      val time = dataChunk.getTime.toString
      val shape =  dataChunk.getShape
      val corner = dataChunk.getCorner
      val varName = dataChunk.getVarName

      val array = tuple._2.getArray
      var cellList = ArrayBuffer.empty[CellOld]

      for (lat:Int <- 0 until shape(0)){
        for (lon:Int <- 0 until shape(1)){
          val index = lat*shape(1)+lon
          val value = array.getShort(index)
          val y = 89.5F - (corner(0)+lat)*1.0F
          val x = -179.5F + (corner(1)+lon)*1.0F
          val cell = CellOld(varName, time, y, x, value)
          cellList += cell
        }
      }
      cellList.toList
    })
  }

  def monthlyAvg(varNum: Int): RDD[(String, Int, Double)] = {
    self.map(tuple => {
      val (sum, count) = sumDouble(tuple._2.getArray, MISSING_VALUE)
      val date = tuple._1.getTime.toString.substring(0, 6)
      val varName = tuple._1.getVarName
      (varName + "_" + date, (sum, count))
    }).reduceByKey{case (tuple1, tuple2) => {
      (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2)
    }}.map( tuple => {
      val components = tuple._1.split("_")
      val varName = components(0)
      val date = components(1).toInt
      val avg = tuple._2._1 / tuple._2._2

      (varName, date, avg)
    })
  }

  def monthlyAvg_v1(varNum: Int): RDD[(String, Int, Double)] = {
    self.map(tuple => {
      val avg = MAMath.sumDouble(tuple._2.getArray)/tuple._2.getArray.getSize
      val date = tuple._1.getTime.toString.substring(0, 6)
      val varName = tuple._1.getVarName
      (varName + "_" + date, avg)
    }).groupByKey().map(tuple => {
      val components = tuple._1.split("_")
      val varName = components(0)
      val date = components(1).toInt
      val avg = tuple._2.sum/tuple._2.size
      (varName, date, avg)
    })
  }

  def average: RDD[(String, Double)] = {
    self.map(tuple => {
      val (sum, count) = sumDouble(tuple._2.getArray, MISSING_VALUE)
      val varName = tuple._1.getVarName
      (varName, (sum, count))
    }).reduceByKey {case (tuple1, tuple2) => {
      (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2)
    }}.map( tuple => {
      val varName = tuple._1
      val avg = tuple._2._1 / tuple._2._2
      (varName, avg)
    })
  }

  def sumDouble(array: ucar.ma2.Array, fillingValue: Float): (Double, Long) = {
    val itor = array.getIndexIterator
    var sum = 0.0
    var count = 0L
    while (itor.hasNext) {
      val cur = itor.getDoubleNext
      if (cur != fillingValue) {
        sum += cur
        count += 1
      }
    }
    (sum, count)
  }

  def timeAvg(varNum: Int): RDD[(String, ArraySerializer)] = {
    val num = (self.count() / varNum).toInt
    self.map(tuple => {
      val key = tuple._1.getVarName
      (key, tuple._2)
    }).reduceByKey((array1, array2) => {
      val a = array1.getArray
      val b = array2.getArray
      val result = ucar.ma2.Array.factory(a.getElementType, a.getShape);

      val iterR = result.getIndexIterator
      val iterA = a.getIndexIterator
      val iterB = b.getIndexIterator

      while (iterA.hasNext()) {
        iterR.setIntNext(iterA.getIntNext + iterB.getIntNext)
      }

      ArraySerializer.factory(result)
    }).mapValues(array => {
      val a = array.getArray
      val result = ucar.ma2.Array.factory(a.getElementType(), a.getShape());
      val iterA = a.getIndexIterator
      val iterR = result.getIndexIterator

      while (iterA.hasNext) {
        iterR.setIntNext(iterA.getIntNext / num)
      }

      ArraySerializer.factory(result)
    })
  }
}

object ClimateRDDFunction {
  implicit def fromRDD(rdd: RDD[(VarKey,ArraySerializer)]): ClimateRDDFunction = new ClimateRDDFunction(rdd)
}
