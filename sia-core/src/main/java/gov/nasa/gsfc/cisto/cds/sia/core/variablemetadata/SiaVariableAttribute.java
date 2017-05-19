package gov.nasa.gsfc.cisto.cds.sia.core.variablemetadata;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.WritableComparable;
import ucar.ma2.InvalidRangeException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * The interface Sia variable attribute.
 */
public interface SiaVariableAttribute extends WritableComparable {

    /**
     * Gets temporal type.
     *
     * @return the temporal type
     */
    public String getTemporalType();

    /**
     * Sets temporal type.
     *
     * @param temporalType the temporal type
     */
    public void setTemporalType(String temporalType);

    /**
     * Gets variable name.
     *
     * @return the variable name
     */
    public String getVariableName();

    /**
     * Sets variable name.
     *
     * @param variableName the variable name
     */
    public void setVariableName(String variableName);

    /**
     * Gets temporal component.
     *
     * @return the temporal component
     */
    public int getTemporalComponent();

    /**
     * Sets temporal component.
     *
     * @param temporalComponent the temporal component
     */
    public void setTemporalComponent(int temporalComponent);

    /**
     * Gets block hosts.
     *
     * @return the block hosts
     */
    public String getBlockHosts();

    /**
     * Sets block hosts.
     *
     * @param blockHosts the block hosts
     */
    public void setBlockHosts(String blockHosts);

    /**
     * Gets compression code.
     *
     * @return the compression code
     */
    public int getCompressionCode();

    /**
     * Sets compression code.
     *
     * @param compressionCode the compression code
     */
    public void setCompressionCode(int compressionCode);

    /**
     * Sets byte offset.
     *
     * @param byteOffset the byte offset
     */
    public void setByteOffset(long byteOffset);

    /**
     * Gets byte offset.
     *
     * @return the byte offset
     */
    public long getByteOffset();

    /**
     * Sets byte length.
     *
     * @param byteLength the byte length
     */
    public void setByteLength(long byteLength);

    /**
     * Gets byte length.
     *
     * @return the byte length
     */
    public long getByteLength();

    /**
    * Gets the array corner.
    *
    * @return the array corner
    */
    public int[] getCorner();

    public void setCorner(int[] corner);

    public void readFields(DataInput dataInput) throws IOException;

    public void write(DataOutput dataOutput) throws IOException;

    public int compareTo(Object object);

    /**
     * Gets variable metadata list.
     *
     * @param <T>        the type parameter
     * @param fileSystem the file system
     * @param file       the file
     * @param variables  the variables
     * @return the variable metadata list
     */
    public <T> List<T> getVariableMetadataList(FileSystem fileSystem, String file, String[] variables);

    /**
     * Variable mapper sia variable attribute.
     *
     * @param variableName         the variable name
     * @param fileSystem           the file system
     * @param fileStatus           the file status
     * @param variableLocationInfo the variable location info
     * @param date                 the date
     * @return the sia variable attribute
     * @throws IOException           the io exception
     * @throws InvalidRangeException the invalid range exception
     * @throws NullPointerException  the null pointer exception
     */
    public SiaVariableAttribute variableMapper(String variableName, FileSystem fileSystem, FileStatus fileStatus, String variableLocationInfo, String date) throws IOException, InvalidRangeException, NullPointerException;

    public String toString();
}
