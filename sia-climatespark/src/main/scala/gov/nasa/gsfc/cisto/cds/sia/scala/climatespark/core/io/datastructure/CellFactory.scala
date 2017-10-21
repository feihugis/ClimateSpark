package gov.nasa.gsfc.cisto.cds.sia.scala.climatespark.core.io.datastructure

import gov.nasa.gsfc.cisto.cds.sia.core.io.key.VarKey
import gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.io.ArraySerializer
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

class CellFactory(self: RDD[(VarKey, ArraySerializer)]) extends Serializable{

  def getCells:RDD[Cell] = {
    self.filter(tuple => tuple._1 != null)
      .flatMap(tuple => {
        val dataChunk = tuple._1
        val time = dataChunk.getTime
        val shape = dataChunk.getShape
        val corner = dataChunk.getCorner
        val varName = dataChunk.getVarName

        val dims = shape.length

        val array = tuple._2.getArray
        val cellList = ArrayBuffer.empty[Cell]

        dims match {
          case 2 =>
            for (lat: Int <- 0 until shape(0)) {
              for (lon: Int <- 0 until shape(1)) {
                val index = lat * shape(1) + lon
                val value = array.getFloat(index)
                val cell = Cell3D(time, corner(0) + lat, corner(1) + lon, value)
                cellList += cell
              }
            }

          case 3 =>
            for (d1: Int <- 0 until shape(0)) {
              for (lat: Int <- 0 until shape(1)) {
                for (lon: Int <- 0 until shape(2)) {
                  val index = d1 * shape(1) * shape(2) + lat * shape(2) + lon
                  val value = array.getFloat(index)
                  val cell = Cell4D(time, corner(0) + d1, corner(1) + lat, corner(2) + lon, value)
                  cellList += cell
                }
              }
            }

          case 4 =>
            for (d1: Int <- 0 until shape(0)) {
              for (d2: Int <- 0 until shape(1)) {
                for (lat: Int <- 0 until shape(2)) {
                  for (lon: Int <- 0 until shape(3)) {
                    val index = d1 * shape(1) * shape(2) * shape(3) + d2 * shape(2) * shape(3) + lat * shape(3) + lon
                    val value = array.getFloat(index)
                    val cell = Cell5D(time, corner(0) + d1, corner(1) + d2, corner(2) + lat, corner(2) + lon, value)
                    cellList += cell
                  }
                }
              }
            }
        }
        cellList.toList
      })
  }
}

object CellFactory {
  implicit def fromDataChunk(rdd: RDD[(VarKey, ArraySerializer)]):CellFactory = new CellFactory(rdd)
}
