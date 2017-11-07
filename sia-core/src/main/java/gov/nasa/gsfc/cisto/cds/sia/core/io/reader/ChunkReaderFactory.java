package gov.nasa.gsfc.cisto.cds.sia.core.io.reader;

import gov.nasa.gsfc.cisto.cds.sia.core.io.SiaChunk;
import ucar.ma2.Array;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * Created by Fei Hu on 11/28/16.
 */
public class ChunkReaderFactory {
  private static final Log LOG = LogFactory.getLog(ChunkReaderFactory.class);

    /**
     * Gets chunk reader.
     *
     * @param dataChunk the data chunk
     * @return the chunk reader
     */
    public static ChunkReader getChunkReader(SiaChunk dataChunk) {

    //0: Merra2; 4: Merra-1;
    switch (dataChunk.getFilterMask()) {
      case 0:
        return new Merra2ChunkReader();
      case 4:
        return new MerraChunkReader();
      case -1:
        return new NonCompressedChunkReader();
    }

    LOG.error("Does not support this datasets :" + dataChunk.toString() );
    return null;
  }
}
