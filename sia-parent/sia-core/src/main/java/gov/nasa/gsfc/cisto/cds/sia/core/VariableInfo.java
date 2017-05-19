package gov.nasa.gsfc.cisto.cds.sia.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * The type Variable info.
 *
 * @author gtamkin
 */
public class VariableInfo implements WritableComparable {

  //TODO:set defaults
	//boundary and climatology vars do not need to use long name or the standard name
	private String standardName;
	private float validMax = 1.0E30F; //CF compliant
	private float validMin = -1.0E30F; //CF compliant 
	private float missingDataFillValue = 9.9999999E14F; //CF compliant
	private int time = 0; //CF compliant
	private long offset = 0L; //CF compliant
	private long length = 0L; //CF compliant
	private String blockHosts; //for hadoop
	private String dataType; //for use by factory, i.e. hdf4, etc.
	private String dateType = ""; //What values can these be?
	private static final Log LOG = LogFactory.getLog(VariableInfo.class);

	/**
	 * Default constructor. Must be implemented in order for hadoop to work
	 */
	public VariableInfo() {
	}

	/**
	 * Instantiates a new Variable info.
	 *
	 * @param dataType the data type
	 */
	public VariableInfo(String dataType) {
		this.dataType = dataType;
	}

	/**
	 * Instantiates a new Variable info.
	 *
	 * @param standardName the standard name
	 * @param dataType     the data type
	 */
	public VariableInfo(String standardName, String dataType) {
		this.standardName = standardName;
		this.dataType = dataType;
	}

	/**
	 * Instantiates a new Variable info.
	 *
	 * @param time        the time
	 * @param byte_offset the byte offset
	 * @param byte_length the byte length
	 * @param block_hosts the block hosts
	 */
	public VariableInfo(int time, long byte_offset, long byte_length, String block_hosts) {
		this.time = time;
		this.offset = byte_offset;
		this.length = byte_length;
		this.blockHosts = block_hosts;
	}

	/**
	 * Instantiates a new Variable info.
	 *
	 * @param standardName the standard name
	 * @param time         the time
	 * @param byte_offset  the byte offset
	 * @param byte_length  the byte length
	 * @param block_hosts  the block hosts
	 */
	public VariableInfo(String standardName, int time, long byte_offset, long byte_length, String block_hosts) {
		this.standardName = standardName;
		this.time = time;
		this.offset = byte_offset;
		this.length = byte_length;
		this.blockHosts = block_hosts;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput in) throws IOException {
		this.standardName = (Text.readString(in));
		this.validMax = in.readFloat();
		this.validMin = in.readFloat();
		this.missingDataFillValue = in.readFloat();
		this.time = in.readInt();
		this.offset = in.readLong();
		this.length = in.readLong();
		this.blockHosts = (Text.readString(in));
		//this.dateType = (Text.readString(in).equals("Monthly") ? DateType.MONTHLY : DateType.DAILY);
		this.dateType = Text.readString(in);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, this.standardName);
		out.writeFloat(this.validMax);
		out.writeFloat(this.validMin);
		out.writeFloat(this.missingDataFillValue);
		out.writeInt(this.time);
		out.writeLong(this.offset);
		out.writeLong(this.length);
		Text.writeString(out, this.blockHosts);
		Text.writeString(out, this.dateType);
	}

	/**
	 * Compare to int.
	 *
	 * @param variableInfo the variable info
	 * @return the int
	 */
/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(VariableInfo variableInfo) {

		return ((!this.blockHosts.equals(variableInfo.getBlockHosts())
				|| !this.standardName.equals(variableInfo.getStandardName()) || this.time != variableInfo.getTime()
				|| this.offset != variableInfo.getOffset() || this.length != variableInfo.getLength()) ? 1 : 0);
	}

	/**
	 * Gets standard name.
	 *
	 * @return the standard name
	 */
	public String getStandardName() {
		return standardName;
	}

	/**
	 * Sets standard name.
	 *
	 * @param standardName the standard name
	 */
	public void setStandardName(String standardName) {
		this.standardName = standardName;
	}

	/**
	 * Gets valid max.
	 *
	 * @return the valid max
	 */
	public float getValidMax() {
		return validMax;
	}

	/**
	 * Sets valid max.
	 *
	 * @param validMax the valid max
	 */
	public void setValidMax(float validMax) {
		this.validMax = validMax;
	}

	/**
	 * Gets valid min.
	 *
	 * @return the valid min
	 */
	public float getValidMin() {
		return validMin;
	}

	/**
	 * Sets valid min.
	 *
	 * @param validMin the valid min
	 */
	public void setValidMin(float validMin) {
		this.validMin = validMin;
	}

	/**
	 * Gets data fill value.
	 *
	 * @return the data fill value
	 */
	public float getmissingDataFillValue() {
		return missingDataFillValue;
	}

	/**
	 * Sets data fill value.
	 *
	 * @param missingDataFillValue the missing data fill value
	 */
	public void setmissingDataFillValue(float missingDataFillValue) {
		this.missingDataFillValue = missingDataFillValue;
	}

	/**
	 * Gets time.
	 *
	 * @return the time
	 */
	public int getTime() {
		return time;
	}

	/**
	 * Sets time.
	 *
	 * @param time the time
	 */
	public void setTime(int time) {
		this.time = time;
	}

	/**
	 * Gets offset.
	 *
	 * @return the offset
	 */
	public long getOffset() {
		return offset;
	}

	/**
	 * Sets offset.
	 *
	 * @param offset the offset
	 */
	public void setOffset(long offset) {
		this.offset = offset;
	}

	/**
	 * Gets length.
	 *
	 * @return the length
	 */
	public long getLength() {
		return length;
	}

	/**
	 * Sets length.
	 *
	 * @param length the length
	 */
	public void setLength(long length) {
		this.length = length;
	}

	/**
	 * Gets block hosts.
	 *
	 * @return the block hosts
	 */
	public String getBlockHosts() {
		return blockHosts;
	}

	/**
	 * Sets block hosts.
	 *
	 * @param blockHosts the block hosts
	 */
	public void setBlockHosts(String blockHosts) {
		this.blockHosts = blockHosts;
	}

	/**
	 * Gets data type.
	 *
	 * @return the data type
	 */
	public String getDataType() {
		return dataType;
	}

	/**
	 * Sets data type.
	 *
	 * @param dataType the data type
	 */
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	/**
	 * Gets date type.
	 *
	 * @return the date type
	 */
	public String getDateType() { return dateType; }

	/**
	 * Sets date type.
	 *
	 * @param dateType the date type
	 */
	public void setDateType(String dateType) {
		this.dateType = dateType;
	}

	/**
	 * Gets variable info audit.
	 *
	 * @return the variable info audit
	 */
	public String getVariableInfoAudit() {
		return "\nVariableInfo Audit: " + "dataType [" + this.dataType + "] " + "standardName [" + this.standardName + "] "
				+ "validMax [" + this.validMax + "] " + "validMin [" + this.validMin + "] "
				+ "missingDataFillValue [" + this.missingDataFillValue + "] " + "time [" + this.time + "] " + "offset [" + this.offset + "] "
				+ "length [" + this.length + "] " + "blockHosts [" + this.blockHosts + "] "
				+ "dateType [" + this.dateType + "]" ;
	}

	/**
	 * Write.
	 *
	 * @param preparedStatement the prepared statement
	 * @throws SQLException the sql exception
	 */
	public void write(PreparedStatement preparedStatement) throws SQLException {
		preparedStatement.setInt(1, time);
		preparedStatement.setLong(2, offset);
		preparedStatement.setLong(3, length);
		preparedStatement.setString(4, blockHosts);
	}

	/**
	 * Read fields.
	 *
	 * @param resultSet the result set
	 * @throws SQLException the sql exception
	 */
	public void readFields(ResultSet resultSet) throws SQLException {
		time = resultSet.getInt(1);
		offset = resultSet.getLong(2);
		length = resultSet.getLong(3);
		blockHosts = resultSet.getString(4);
	}

	public int compareTo(Object objt) {
		// TODO Auto-generated method stub
		VariableInfo varInfo = (VariableInfo) objt;
		if( !this.blockHosts.equals(varInfo.blockHosts)) {
			return 1;
		} else {
			if( !this.standardName.equals(varInfo.getStandardName())) {
				return 1;
			} else {
				if( this.time != varInfo.getTime()) {
					return 1;
				} else {
					if( this.offset != varInfo.getOffset()) {
						return 1;
					} else {
						if( this.length != varInfo.getLength()) {
							return 1;
						} else {
							return 0;
						}
					}
				}
			}
		}

	}
}
