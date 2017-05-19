package gov.nasa.gsfc.cisto.cds.sia.core.io.reader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import ucar.unidata.io.RandomAccessFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


/**
 * The type Nc hdfs raf.
 */
public class NcHdfsRaf
    extends RandomAccessFile {

  private static final Log LOG = LogFactory.getLog(NcHdfsRaf.class);
  private FSDataInputStream _hdfs_file = null;
  private long total_length = 0L;
  private FileSystem _fs = null;
  private float localDataNum = 0.0F;
  private float totalDataNum = 0.0F;
  private String hostname;


    /**
     * Instantiates a new Nc hdfs raf.
     *
     * @param fileStatus the file status
     * @param job        the job
     * @param bufferSize the buffer size
     * @throws IOException the io exception
     */
    public NcHdfsRaf(FileStatus fileStatus, Configuration job, int bufferSize)
      throws IOException {
    super(bufferSize);

    Path path = fileStatus.getPath();
    this.location = path.toString();
    this.total_length = fileStatus.getLen();
    openFiles.add(this.location);

    this._fs = path.getFileSystem(job);
    this._hdfs_file = this._fs.open(path);

    this.hostname = getLocalhost();
  }


    /**
     * Instantiates a new Nc hdfs raf.
     *
     * @param fileStatus the file status
     * @param job        the job
     * @throws IOException the io exception
     */
    public NcHdfsRaf(FileStatus fileStatus, Configuration job)
      throws IOException {
    this(fileStatus, job, 8092);
  }


  protected int read_(long pos, byte[] buf, int offset, int len)
      throws IOException {
    int n = this._hdfs_file.read(pos, buf, offset, len);

    BlockLocation[]
        blockLocations =
        this._fs.getFileBlockLocations(this._fs.getFileStatus(new Path(this.location)), offset, len);
    String[] hosts = blockLocations[0].getHosts();
    String location = "";
    for (String host : hosts) {
      location = location + host + ",";
    }

    this.totalDataNum += len;
    if (this.hostname.equals(hosts[0])) {
      this.localDataNum += len;
    }
    return n;
  }


  public long readToByteChannel(WritableByteChannel dest, long offset, long nbytes)
      throws IOException {
    int n = (int) nbytes;
    byte[] buf = new byte[n];
    int done = read_(offset, buf, 0, n);
    dest.write(ByteBuffer.wrap(buf));
    return done;
  }


  public long length()
      throws IOException {
    if (this.total_length < this.dataEnd) {
      return this.dataEnd;
    }
    return this.total_length;
  }


  public void close()
      throws IOException {
    this._hdfs_file.close();
    super.close();
  }


    /**
     * Gets localhost.
     *
     * @return the localhost
     * @throws IOException the io exception
     */
    public String getLocalhost() throws IOException {
    byte[] localhost = new byte[30];
    Runtime.getRuntime().exec("hostname").getInputStream().read(localhost);
    String hostname = new String(localhost);
    hostname = hostname.split("\n")[0];
    return hostname;
  }


    /**
     * Gets local data num.
     *
     * @return the local data num
     */
    public float getLocalDataNum() {
    return this.localDataNum;
  }


    /**
     * Gets total data num.
     *
     * @return the total data num
     */
    public float getTotalDataNum() {
    return this.totalDataNum;
  }
}


/* Location:              /Users/mkbowen/Desktop/MerraTest.jar!/edu/gmu/stc/merra/hadoop/io/NcHdfsRaf.class
 * Java compiler version: 6 (50.0)
 * JD-Core Version:       0.7.1
 */