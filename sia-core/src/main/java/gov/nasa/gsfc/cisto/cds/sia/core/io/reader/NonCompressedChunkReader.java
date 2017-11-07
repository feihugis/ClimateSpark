package gov.nasa.gsfc.cisto.cds.sia.core.io.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import gov.nasa.gsfc.cisto.cds.sia.core.io.SiaChunk;
import ucar.ma2.Array;

/**
 * Created by Fei Hu on 11/6/17.
 */
public class NonCompressedChunkReader extends ChunkReader{

  public Array read(SiaChunk dataChunk, byte[] compressedByteArray) throws IOException {
    ByteBuffer bb = ByteBuffer.wrap(compressedByteArray);
    bb.order(ByteOrder.LITTLE_ENDIAN);
    return convertByteBuffer2Array(bb, dataChunk.getDataType(), dataChunk.getShape());
  }
}
