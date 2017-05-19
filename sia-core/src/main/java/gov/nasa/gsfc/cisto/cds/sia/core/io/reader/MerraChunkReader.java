package gov.nasa.gsfc.cisto.cds.sia.core.io.reader;

import gov.nasa.gsfc.cisto.cds.sia.core.io.SiaChunk;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.nc2.util.IO;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Created by Fei Hu on 11/28/16.
 */
public class MerraChunkReader extends ChunkReader {

  @Override
  public Array read(SiaChunk dataChunk, byte[] compressedByteArray) throws IOException {
    InputStream in = new ByteArrayInputStream(compressedByteArray);
    InputStream zin = new java.util.zip.InflaterInputStream(in);
    int typeLength = sizeof(DataType.getType(dataChunk.getDataType()).getClassType());
    ByteArrayOutputStream out = new ByteArrayOutputStream(dataChunk.getShapeSize() * typeLength);
    IO.copy(zin, out);
    byte[] buffer = out.toByteArray();
    ByteBuffer bb = ByteBuffer.wrap(buffer);
    return convertByteBuffer2Array(bb, dataChunk.getDataType(), dataChunk.getShape());
  }
}
