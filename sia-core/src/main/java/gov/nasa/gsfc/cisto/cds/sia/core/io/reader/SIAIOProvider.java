package gov.nasa.gsfc.cisto.cds.sia.core.io.reader;

import gov.nasa.gsfc.cisto.cds.sia.core.io.SiaChunk;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import ucar.ma2.Array;

import java.io.IOException;

/**
 * Created by Fei Hu on 9/14/16.
 */
public class SIAIOProvider {
  private static final Log LOG = LogFactory.getLog(SIAIOProvider.class);

    /**
     * Read array.
     *
     * @param dataChunk   the data chunk
     * @param inputStream the input stream
     * @return the array
     * @throws IOException the io exception
     */
    public static Array read(SiaChunk dataChunk, FSDataInputStream inputStream) throws IOException {
    byte[] compressedByteArray = new byte[(int) dataChunk.getByteSize()];
    long filePos = dataChunk.getFilePos();
    int n = inputStream.read(filePos, compressedByteArray, 0, compressedByteArray.length);

    ChunkReader chunkReader = ChunkReaderFactory.getChunkReader(dataChunk);

    if (chunkReader != null) {
      return chunkReader.read(dataChunk, compressedByteArray);
    } else {
      LOG.error("Does not support this datasets :" + dataChunk.toString() );
      return null;
    }
  }

}
