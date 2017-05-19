package gov.nasa.gsfc.cisto.cds.sia.core.io;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Fei Hu on 9/15/16.
 */
public class SiaChunk implements Writable, KryoSerializable {
  //@Id @GeneratedValue(strategy = GenerationType.AUTO)
  private int id;
  private int[] corner = null;         //relative to the whole picture
  private int[] shape = null;          //chunk shape; to get the endcorner: corner[0] + shape[0] - 1
  private String[] dimensions = null;  //dimension info  for each dimension, such [time, lat, lon]
  private long filePos = 0;            //the start location in the file
  private long byteSize = 0;           // byte size for this chunk
  private int filterMask = -1;         //compression type for HDF4; filter type for HDF5  TODO:: filterMask is confusing
  private String[] hosts = null;       //the data nodes who host these data
  private String dataType;             //the data type
  private String varShortName;
  private String filePath;
  private boolean isContain = true;
  private int time = 0;
    /**
     * The Geometry info.
     */
    String geometryInfo = "";

  //TODO:: add more attributes, such as collection; looking at the notes


    /**
     * Instantiates a new Sia chunk.
     */
    public SiaChunk() {}

    /**
     * Instantiates a new Sia chunk.
     *
     * @param corner       the corner
     * @param shape        the shape
     * @param dimensions   the dimensions
     * @param filePos      the file pos
     * @param byteSize     the byte size
     * @param filterMask   the filter mask
     * @param hosts        the hosts
     * @param dataType     the data type
     * @param varShortName the var short name
     * @param filePath     the file path
     * @param time         the time
     * @param geometryInfo the geometry info
     */
    public SiaChunk(int[] corner, int[] shape, String[] dimensions, long filePos, long byteSize,
                  int filterMask, String[] hosts, String dataType, String varShortName, String filePath, int time, String geometryInfo) {
    this.corner = corner;
    this.shape = shape;
    this.dimensions = dimensions;
    this.filePos = filePos;
    this.byteSize = byteSize;
    this.filterMask = filterMask;
    this.hosts = hosts;
    this.dataType = dataType;
    this.varShortName = varShortName;
    this.filePath = filePath;
    this.time = time;
    this.geometryInfo = geometryInfo;
  }

  @Override
  public String toString() {
    String output = this.getVarShortName() + " corner : ";
    for (int corner : getCorner()) {
      output = output + corner + " ";
    }
    output += "start : " + getFilePos() + " end : " + (getFilePos() + getByteSize());
    return output;
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(corner.length);
    for (int i=0; i<corner.length; i++) {
      out.writeInt(corner[i]);
    }

    for (int i=0; i<shape.length; i++) {
      out.writeInt(shape[i]);
    }

    for (int i=0; i<dimensions.length; i++) {
      Text.writeString(out, dimensions[i]);
    }

    out.writeLong(filePos);
    out.writeLong(byteSize);
    out.writeInt(filterMask);

    out.writeInt(hosts.length);
    for (int i=0; i<hosts.length; i++) {
      Text.writeString(out, hosts[i]);
    }

    Text.writeString(out, dataType);
    Text.writeString(out, varShortName);
    Text.writeString(out, filePath);

    out.writeBoolean(isContain);
    out.writeInt(this.time);

    Text.writeString(out, geometryInfo);
  }

  public void readFields(DataInput in) throws IOException {
    int num = in.readInt();
    corner = new int[num];
    shape = new int[num];
    dimensions = new String[num];

    for (int i=0; i<num; i++) {
      corner[i] = in.readInt();
    }

    for (int i=0; i<num; i++) {
      shape[i] = in.readInt();
    }

    for (int i=0; i<num; i++) {
      dimensions[i] = Text.readString(in);
    }

    filePos = in.readLong();
    byteSize = in.readLong();
    filterMask = in.readInt();

    num = in.readInt();
    hosts = new String[num];
    for (int i =0; i<num; i++) {
      hosts[i] = Text.readString(in);
    }

    dataType = Text.readString(in);
    varShortName = Text.readString(in);
    filePath = Text.readString(in);
    isContain = in.readBoolean();
    this.time = in.readInt();
    this.geometryInfo = Text.readString(in);
  }

  public void write(Kryo kryo, Output out) {
    kryo.writeObjectOrNull(out, this, this.getClass());
    out.writeInt(corner.length);
    for (int i=0; i<corner.length; i++) {
      out.writeInt(corner[i]);
    }

    for (int i=0; i<shape.length; i++) {
      out.writeInt(shape[i]);
    }

    for (int i=0; i<dimensions.length; i++) {
      out.writeString(dimensions[i]);
    }

    out.writeLong(filePos);
    out.writeLong(byteSize);
    out.writeInt(filterMask);

    out.writeInt(hosts.length);
    for (int i=0; i<hosts.length; i++) {
      out.writeString(hosts[i]);
    }

    out.writeString(dataType);
    out.writeString(varShortName);
    out.writeString(filePath);

    out.writeBoolean(isContain);
  }

  public void read(Kryo kryo, Input in) {
    kryo.readObjectOrNull(in, this.getClass());
    int num = in.readInt();
    corner = new int[num];
    shape = new int[num];
    dimensions = new String[num];

    for (int i=0; i<num; i++) {
      corner[i] = in.readInt();
    }

    for (int i=0; i<num; i++) {
      shape[i] = in.readInt();
    }

    for (int i=0; i<num; i++) {
      dimensions[i] = in.readString();
    }

    filePos = in.readLong();
    byteSize = in.readLong();
    filterMask = in.readInt();

    num = in.readInt();
    hosts = new String[num];
    for (int i =0; i<num; i++) {
      hosts[i] = in.readString();
    }

    dataType = in.readString();
    varShortName = in.readString();
    filePath = in.readString();
    isContain = in.readBoolean();
  }

    /**
     * Is contain boolean.
     *
     * @return the boolean
     */
    public boolean isContain() {
    return isContain;
  }

    /**
     * Sets contain.
     *
     * @param contain the contain
     */
    public void setContain(boolean contain) {
    isContain = contain;
  }

    /**
     * Get corner int [ ].
     *
     * @return the int [ ]
     */
    public int[] getCorner() {
    return corner;
  }

    /**
     * Sets corner.
     *
     * @param corner the corner
     */
    public void setCorner(int[] corner) {
    this.corner = corner;
  }

    /**
     * Get shape int [ ].
     *
     * @return the int [ ]
     */
    public int[] getShape() {
    return shape;
  }

    /**
     * Gets shape size.
     *
     * @return the shape size
     */
    public int getShapeSize() {
    int size = 1;
    for (int i : shape) {
      size = size * i;
    }
    return size;
  }

    /**
     * Sets shape.
     *
     * @param shape the shape
     */
    public void setShape(int[] shape) {
    this.shape = shape;
  }

    /**
     * Get dimensions string [ ].
     *
     * @return the string [ ]
     */
    public String[] getDimensions() {
    return dimensions;
  }

    /**
     * Sets dimensions.
     *
     * @param dimensions the dimensions
     */
    public void setDimensions(String[] dimensions) {
    this.dimensions = dimensions;
  }

    /**
     * Gets file pos.
     *
     * @return the file pos
     */
    public long getFilePos() {
    return filePos;
  }

    /**
     * Sets file pos.
     *
     * @param filePos the file pos
     */
    public void setFilePos(long filePos) {
    this.filePos = filePos;
  }

    /**
     * Gets byte size.
     *
     * @return the byte size
     */
    public long getByteSize() {
    return byteSize;
  }

    /**
     * Sets byte size.
     *
     * @param byteSize the byte size
     */
    public void setByteSize(long byteSize) {
    this.byteSize = byteSize;
  }

    /**
     * Gets filter mask.
     *
     * @return the filter mask
     */
    public int getFilterMask() {
    return filterMask;
  }

    /**
     * Sets filter mask.
     *
     * @param filterMask the filter mask
     */
    public void setFilterMask(int filterMask) {
    this.filterMask = filterMask;
  }

    /**
     * Get hosts string [ ].
     *
     * @return the string [ ]
     */
    public String[] getHosts() {
    return hosts;
  }

    /**
     * Sets hosts.
     *
     * @param hosts the hosts
     */
    public void setHosts(String[] hosts) {
    this.hosts = hosts;
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
     * Gets var short name.
     *
     * @return the var short name
     */
    public String getVarShortName() {
    return varShortName;
  }

    /**
     * Sets var short name.
     *
     * @param varShortName the var short name
     */
    public void setVarShortName(String varShortName) {
    this.varShortName = varShortName;
  }

    /**
     * Gets file path.
     *
     * @return the file path
     */
    public String getFilePath() {
    return filePath;
  }

    /**
     * Sets file path.
     *
     * @param filePath the file path
     */
    public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

    /**
     * Gets time.
     *
     * @return the time
     */
    public int getTime() {return  time; }

    /**
     * Sets time.
     *
     * @param time the time
     */
    public void setTime(int time) { this.time = time;}

    /**
     * Gets geometry info.
     *
     * @return the geometry info
     */
    public String getGeometryInfo() {
    return geometryInfo;
  }

    /**
     * Sets geometry info.
     *
     * @param geometryInfo the geometry info
     */
    public void setGeometryInfo(String geometryInfo) {
    this.geometryInfo = geometryInfo;
  }

    /**
     * Gets id.
     *
     * @return the id
     */
    public int getId() {
    return id;
  }

    /**
     * Sets id.
     *
     * @param id the id
     */
    public void setId(int id) {
    this.id = id;
  }

}
