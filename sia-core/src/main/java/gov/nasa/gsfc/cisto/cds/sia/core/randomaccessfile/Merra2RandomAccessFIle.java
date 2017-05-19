package gov.nasa.gsfc.cisto.cds.sia.core.randomaccessfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;

/**
 * The type Merra 2 random access f ile.
 */
public class Merra2RandomAccessFIle extends MerraRandomAccessFile {

    /**
     * Instantiates a new Merra 2 random access f ile.
     *
     * @param fileStatus the file status
     * @param job        the job
     * @throws IOException the io exception
     */
    public Merra2RandomAccessFIle(FileStatus fileStatus,
                                Configuration job) throws IOException {
    super(fileStatus, job);
  }
}
