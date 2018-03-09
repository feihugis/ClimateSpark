package gov.nasa.gsfc.cisto.cds.sia.core.randomaccessfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import ucar.unidata.io.RandomAccessFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * The type Merra random access file.
 */
public class MerraRandomAccessFile extends RandomAccessFile {

    private FSDataInputStream hdfsFile = null;
    private long totalLength = 0L;
    private FileSystem fileSystem = null;
    private float localHostDataRead = 0.0F;
    private float totalDataRead = 0.0F;
    private String hostname;


    /**
     * Instantiates a new Merra random access file.
     *
     * @param fileStatus the file status
     * @param job        the job
     * @param bufferSize the buffer size
     * @throws IOException the io exception
     */
    public MerraRandomAccessFile(FileStatus fileStatus, Configuration job, int bufferSize)
            throws IOException {
        super(bufferSize);

        Path path = fileStatus.getPath();
        this.location = path.toString();
        this.totalLength = fileStatus.getLen();
        openFiles.add(this.location);

        this.fileSystem = path.getFileSystem(job);
        this.hdfsFile = this.fileSystem.open(path);

        this.hostname = getLocalhost();
    }


    /**
     * Instantiates a new Merra random access file.
     *
     * @param fileStatus the file status
     * @param job        the job
     * @throws IOException the io exception
     */
    public MerraRandomAccessFile(FileStatus fileStatus, Configuration job) throws IOException {
        this(fileStatus, job, 8092);
    }

    /**
     *
     * @param pos the position to start at in the file
     * @param buf the byte[] buffer to place data in
     * @param offset the buffer offset
     * @param len the number of bytes to read
     * @return the actual number of bytes read
     * @throws IOException
     */
    @Override
    protected int read_(long pos, byte[] buf, int offset, int len) throws IOException {
        int n = this.hdfsFile.read(pos, buf, offset, len);
        return n;
    }

    /**
     *
     * @param dest write to this WritableByteChannel
     * @param offset the offset in the file where copying will start
     * @param nbytes the number of bytes to read
     * @return the actual number of bytes read and transferred
     * @throws IOException
     */
    @Override
    public long readToByteChannel(WritableByteChannel dest, long offset, long nbytes) throws IOException {
        int n = (int) nbytes;
        byte[] buf = new byte[n];
        int done = read_(offset, buf, 0, n);
        dest.write(ByteBuffer.wrap(buf));
        return done;
    }

    /**
     *
     * @return the length of the file
     * @throws IOException
     */
    @Override
    public long length()
            throws IOException {
        if (this.totalLength < this.dataEnd) {
            return this.dataEnd;
        }
        return this.totalLength;
    }

    /**
     * Close the file, and release any associated system resources
     * @throws IOException
     */
    @Override
    public void close()
            throws IOException {
        this.hdfsFile.close();
        super.close();
    }

    /**
     * Gets localhost.
     *
     * @return the local host
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
     * Gets local host data read.
     *
     * @return size of the localHost data read
     */
    public float getLocalHostDataRead() {
        return this.localHostDataRead;
    }

    /**
     * Gets total data read.
     *
     * @return size of the total data read
     */
    public float getTotalDataRead() {
        return this.totalDataRead;
    }

}
