package gov.nasa.gsfc.cisto.cds.sia.core.io.reader;

import gov.nasa.gsfc.cisto.cds.sia.core.io.SiaChunk;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.nc2.util.IO;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Created by Fei Hu on 11/28/16.
 */
public class Merra2ChunkReader extends ChunkReader {

  @Override
  public Array read(SiaChunk dataChunk, byte[] compressedByteArray) throws IOException {
    compressedByteArray = inflate(compressedByteArray);
    int typeLength = sizeof(DataType.getType(dataChunk.getDataType()).getClassType());
    compressedByteArray = shuffle(compressedByteArray, typeLength);
    ByteBuffer bb = ByteBuffer.wrap(compressedByteArray);
    ByteOrder byteOrder = ByteOrder.LITTLE_ENDIAN;
    bb.order(byteOrder);
    return convertByteBuffer2Array(bb, dataChunk.getDataType(), dataChunk.getShape());
  }

  /**
   * inflate data
   *
   * @param compressed compressed data
   * @return uncompressed data
   * @throws IOException on I/O error
   */
  private static byte[] inflate(byte[] compressed) throws IOException {
    // run it through the Inflator
    ByteArrayInputStream in = new ByteArrayInputStream(compressed);
    java.util.zip.InflaterInputStream inflater = new java.util.zip.InflaterInputStream(in);
    ByteArrayOutputStream out = new ByteArrayOutputStream(8 * compressed.length);
    IO.copy(inflater, out);

    byte[] uncomp = out.toByteArray();
    return uncomp;
  }

  private static byte[] shuffle(byte[] data, int n) throws IOException {
    assert data.length % n == 0;
    if (n <= 1) return data;

    int m = data.length / n;
    int[] count = new int[n];
    for (int k = 0; k < n; k++) count[k] = k * m;

    byte[] result = new byte[data.length];

    for (int i = 0; i < m; i++) {
      for (int j = 0; j < n; j++) {
        result[i*n+j] = data[i + count[j]];
      }
    }

    return result;
  }
}
