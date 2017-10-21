package gov.nasa.gsfc.cisto.cds.sia.core.io.reader;

import gov.nasa.gsfc.cisto.cds.sia.core.io.SiaChunk;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import ucar.ma2.Array;
import ucar.ma2.DataType;

import java.io.IOException;
import java.nio.*;

/**
 * Created by Fei Hu on 11/28/16.
 */
public abstract class ChunkReader {
  private static final Log LOG = LogFactory.getLog(ChunkReader.class);

    /**
     * Read array.
     *
     * @param dataChunk           the data chunk
     * @param compressedByteArray the compressed byte array
     * @return the array
     * @throws IOException the io exception
     */
    public abstract Array read(SiaChunk dataChunk, byte[] compressedByteArray) throws IOException;

    /**
     * Convert byte buffer 2 array array.
     *
     * @param bb       the bb
     * @param dataType the data type
     * @param shape    the shape
     * @return the array
     */
    public static Array convertByteBuffer2Array(ByteBuffer bb, String dataType, int[] shape) {
    if (dataType.equals("short")) {
      ShortBuffer valBuffer = bb.asShortBuffer();
      short[] pa = new short[valBuffer.capacity()];
      for (int i = 0; i < pa.length; i++) {
        pa[i] = valBuffer.get(i);
      }
      return Array.factory(DataType.getType(dataType), shape, pa);
    }

    if (dataType.equals("int")) {
      IntBuffer valBuffer = bb.asIntBuffer();
      int[] pa = new int[valBuffer.capacity()];
      for (int i = 0; i < pa.length; i++) {
        pa[i] = valBuffer.get(i);
      }
      return Array.factory(DataType.getType(dataType), shape, pa);
    }

    if (dataType.equals("float")) {
      FloatBuffer valBuffer = bb.asFloatBuffer();
      float[] pa = new float[valBuffer.capacity()];
      for (int i = 0; i < pa.length; i++) {
        pa[i] = valBuffer.get(i);
      }
      return Array.factory(DataType.getType(dataType), shape, pa);
    }

    if (dataType.equals("double")) {
      DoubleBuffer valBuffer = bb.asDoubleBuffer();
      double[] pa = new double[valBuffer.capacity()];
      for (int i = 0; i < pa.length; i++) {
        pa[i] = valBuffer.get(i);
      }
      return Array.factory(DataType.getType(dataType), shape, pa);
    }

    LOG.error("Does not support this data type : " + dataType + "  in class: " + SIAIOProvider.class.getName());

    return null;
  }

    /**
     * Sizeof int.
     *
     * @param dataType the data type
     * @return the int
     */
    public static int sizeof(Class dataType) {
    if (dataType == null) throw new NullPointerException();

    if (dataType == int.class    || dataType == Integer.class)   return 4;
    if (dataType == short.class  || dataType == Short.class)     return 2;
    if (dataType == byte.class   || dataType == Byte.class)      return 1;
    if (dataType == char.class   || dataType == Character.class) return 2;
    if (dataType == long.class   || dataType == Long.class)      return 8;
    if (dataType == float.class  || dataType == Float.class)     return 4;
    if (dataType == double.class || dataType == Double.class)    return 8;

    return 4;
  }
}
