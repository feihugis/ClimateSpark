package gov.nasa.gsfc.cisto.cds.sia.core.variablemetadata;

import gov.nasa.gsfc.cisto.cds.sia.core.randomaccessfile.MerraRandomAccessFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Fei Hu on 8/17/16.
 */
public class Merra2VariableAttribute implements SiaVariableAttribute, WritableComparable {
  private String datasetName;
  private String fileExtension;
  private String hibernateConfigXmlFile = "merra2.cfg.xml";
  private String temporalType;
  private float validMin = -1.0E30F;
  private float validMax = -1.0E30F;

  private float fillValue = 9.9999999E14F;
  private int temporalComponent = 0;

  private int[] corner = null;         //relative to the whole picture
  private int[] shape = null;          //chunk shape; to get the endcorner: corner[0] + shape[0] - 1
  private String[] dimensions = null;  //dimension info for each dimension, such [time, lat, lon]
  private long filePos = 0;            //the start location in the file
  private long byteSize = 0;           // byte size for this chunk
  private int filterMask = 1;         //compression type for HDF4; filter type for HDF5
  private String blockHosts = null;    //the data nodes who host these data
  private String dataType;             //the data type
  private String varShortName;
  private String filePath;
  private int geometryID = 0;


    /**
     * Instantiates a new Merra 2 variable attribute.
     *
     * @param datasetName       the dataset name
     * @param fileExtension     the file extension
     * @param temporalType      the temporal type
     * @param temporalComponent the temporal component
     * @param corner            the corner
     * @param shape             the shape
     * @param dimensions        the dimensions
     * @param filePos           the file pos
     * @param byteSize          the byte size
     * @param filterMask        the filter mask
     * @param blockHosts        the block hosts
     * @param dataType          the data type
     * @param varShortName      the var short name
     * @param filePath          the file path
     */
    public Merra2VariableAttribute(String datasetName, String fileExtension,
                                 String temporalType, int temporalComponent,
                                 int[] corner, int[] shape, String[] dimensions, long filePos, long byteSize,
                                 int filterMask,
                                 String blockHosts, String dataType, String varShortName,
                                 String filePath) {
    this.datasetName = datasetName;
    this.fileExtension = fileExtension;
    this.temporalType = temporalType;
    this.temporalComponent = temporalComponent;
    this.corner = corner;
    this.shape = shape;
    this.dimensions = dimensions;
    this.filePos = filePos;
    this.byteSize = byteSize;
    this.filterMask = filterMask;
    this.blockHosts = blockHosts;
    this.dataType = dataType;
    this.varShortName = varShortName;
    this.filePath = filePath;
    this.geometryID = this.calGeometryID();
  }

  public void readFields(DataInput dataInput) throws IOException {
    int num = dataInput.readInt();
    corner = new int[num];
    shape = new int[num];
    dimensions = new String[num];

    for (int i=0; i<num; i++) {
      corner[i] = dataInput.readInt();
    }

    for (int i=0; i<num; i++) {
      shape[i] = dataInput.readInt();
    }

    for (int i=0; i<num; i++) {
      dimensions[i] = Text.readString(dataInput);
    }

    filePos = dataInput.readLong();
    byteSize = dataInput.readLong();
    filterMask = dataInput.readInt();

    this.blockHosts = Text.readString(dataInput);
    this.dataType = Text.readString(dataInput);
    this.varShortName = Text.readString(dataInput);
    this.filePath = Text.readString(dataInput);
    this.temporalComponent = dataInput.readInt();
    this.datasetName = Text.readString(dataInput);
    this.fileExtension = Text.readString(dataInput);
    this.hibernateConfigXmlFile = Text.readString(dataInput);
    this.temporalType = Text.readString(dataInput);
    this.validMin = dataInput.readFloat();
    this.validMax = dataInput.readFloat();
    this.fillValue = dataInput.readFloat();
    this.geometryID = dataInput.readInt();
  }

  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(this.corner.length);
    for (int i=0; i<this.corner.length; i++) {
      dataOutput.writeInt(this.corner[i]);
    }

    for (int i=0; i<this.shape.length; i++) {
      dataOutput.writeInt(this.shape[i]);
    }

    for (int i=0; i<this.dimensions.length; i++) {
      Text.writeString(dataOutput, this.dimensions[i]);
    }

    dataOutput.writeLong(this.filePos);
    dataOutput.writeLong(this.byteSize);
    dataOutput.writeInt(this.filterMask);

    Text.writeString(dataOutput, this.blockHosts);

    Text.writeString(dataOutput, this.dataType);
    Text.writeString(dataOutput, this.varShortName);
    Text.writeString(dataOutput, this.filePath);

    dataOutput.writeInt(this.temporalComponent);

    Text.writeString(dataOutput, this.datasetName);
    Text.writeString(dataOutput, this.fileExtension);
    Text.writeString(dataOutput, this.hibernateConfigXmlFile);
    Text.writeString(dataOutput, this.temporalType);
    dataOutput.writeFloat(this.validMin);
    dataOutput.writeFloat(this.validMax);
    dataOutput.writeFloat(this.fillValue);
    dataOutput.writeInt(this.geometryID);
  }

  public int compareTo(Object object) {
    return 0;
  }

  public List<SiaVariableAttribute> getVariableMetadataList(FileSystem fileSystem, String file, String[] variables) {
    List<SiaVariableAttribute> siaVariableAttributeList = new ArrayList<SiaVariableAttribute>();
    try {
      // FileStatus object -
      // isDirectory, length, replication, blocksize, modification_time, access_time, owner, group, permission, isSymlink
      FileStatus fileStatus = fileSystem.getFileStatus(new Path(file));

      // RandomAccessFile object -
      // just file path?
      MerraRandomAccessFile randomAccessFile = new MerraRandomAccessFile(fileStatus, new Configuration());

      // NetcdfFile object -
      // ALL of the netcdf file header data, list of variables, dimensions, etc...
      NetcdfFile netCdfFile = NetcdfFile.open(randomAccessFile, fileStatus.getPath().toString());

      // List of the variables contained in the netcdf file
      List<Variable> varListFromFile = netCdfFile.getVariables();

      for(Variable variableFromFile : varListFromFile) {
        for (String indexVar : variables) {
          String variableToBeIndexed = indexVar.replaceAll("\\s", "");

          if ((variableFromFile.getShortName().equals(variableToBeIndexed)) || (variableFromFile.getFullName().equals(variableToBeIndexed))) {
            //getVarLocationInformation returns something like:
            //0:0,0:0,0:360,0:539 start at 8894171, length is 153067; 0:0,1:1,0:360,0:539 start at 9051400, length is 215556; ...
            //first-dim,second-dim,third-dim,fourth-dim byte-offset, byte-length;...
            //the byte-offset and length are the offset and length into the file for the variable
            String variableFromFileLocationInfo = variableFromFile.getVarLocationInformation();

            String[] chunks = variableFromFileLocationInfo.split(";");

            for (String chunk : chunks) {
              SiaVariableAttribute siaVariableAttribute = variableMapper(fileSystem,chunk, variableFromFile.getDimensionsString(),
                                                                       variableFromFile.getShortName(), file,
                                                                       variableFromFile.getDataType().toString());
              siaVariableAttributeList.add(siaVariableAttribute);
            }
          }

        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return siaVariableAttributeList;
  }

    /**
     * TODO: fix the time extraction, here it treat the last second one as the time info
     *
     * @param fileSystem   the file system
     * @param metaInfo     the meta info
     * @param dimensions   the dimensions
     * @param varShortName the var short name
     * @param filePath     the file path
     * @param dataType     the data type
     * @return sia variable attribute
     * @throws IOException the io exception
     */
//0:0,0:90,288:431  ChunkedDataNode size=37348 filterMask=0 filePos=72345715 offsets= 0 0 288 0
  public SiaVariableAttribute variableMapper(FileSystem fileSystem, String metaInfo, String dimensions, String varShortName, String filePath, String dataType)
      throws IOException {
    String cnr = metaInfo.substring(0, metaInfo.indexOf("  ChunkedDataNode"));
    String[] tmps = filePath.split("\\.");
    int time = Integer.parseInt(tmps[tmps.length-2]);

    String byteSizeIn = subString(metaInfo, "size=", " filterMask=");
    String filterMaskIn = subString(metaInfo, "filterMask=", " filePos=");
    String filePosIn = subString(metaInfo, "filePos=", " offsets");

    String[] cnrs = cnr.split(",");
    int corners[] = new int[cnrs.length];
    int shape[] = new int[cnrs.length];
    for (int i=0; i<cnrs.length; i++) {
      String[] r = cnrs[i].split(":");
      corners[i] = Integer.parseInt(r[0]);
      shape[i] = Integer.parseInt(r[1]) - Integer.parseInt(r[0]) + 1;
    }

    long byteSize = Long.parseLong(byteSizeIn);
    long filePos = Long.parseLong(filePosIn);
    int filterMask = Integer.parseInt(filterMaskIn);

    String[] dims = dimensions.split(" ");

    BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileSystem.getFileLinkStatus(new Path(filePath)), filePos, byteSize);
    List<String> hosts = new ArrayList<String>();
    for (BlockLocation blck: blockLocations) {
      hosts.addAll(Arrays.asList(blck.getHosts()));
    }

    String hostString = "";
    for (int i=0; i<hosts.size(); i++) {
      if (i == (hosts.size()-1)) {
        hostString = hostString + hosts.get(i);
      } else {
        hostString = hostString + hosts.get(i) + ",";
      }
    }

    Merra2VariableAttribute chunkMetadata = new Merra2VariableAttribute("", "nc4", "hourly", time, corners, shape, dims, filePos,
                                                   byteSize, filterMask, hostString,
                                                   dataType, varShortName, filePath);

    return chunkMetadata;
  }


  public SiaVariableAttribute variableMapper(String variableName, FileSystem fileSystem,
                                             FileStatus fileStatus, String variableLocationInfo,
                                             String date)
      throws IOException, InvalidRangeException, NullPointerException {
    return null;
  }

    /**
     * Cal geometry id int.
     *
     * @return the int
     */
//TODO: need fill MERRA2 meta data information
   public int calGeometryID() {
    int latMark = -1, lonMark = -1;
    for (int i = 0; i<this.getDimensions().length; i++) {
      if (this.getDimensions()[i].equals("lat")) latMark = i;
      if (this.getDimensions()[i].equals("lon")) lonMark = i;
    }

    if (latMark == -1 || lonMark == -1) {
      return 0;
    }

    int corner_lat = this.getCorner()[latMark];
    int corner_lon = this.getCorner()[lonMark];
    int latChunkShape = 91, lonChunkShape = 144;
    int id = corner_lat/latChunkShape*4 + corner_lon/lonChunkShape;
    return id;
  }

    /**
     * get the subString between 'start' and 'end' in the 'input'
     *
     * @param input the string to be subsetted
     * @param start the start chars
     * @param end   the end chars
     * @return string
     */
    public static String subString(String input, String start, String end) {
    return input.substring(input.indexOf(start) + start.length(), input.indexOf(end));
  }

    /**
     * Gets collection name.
     *
     * @return the collection name
     */
    public String getCollectionName() {
    return null;
  }

    /**
     * Sets collection name.
     *
     * @param collectionName the collection name
     */
    public void setCollectionName(String collectionName) {

  }

  public String getTemporalType() {
    return null;
  }

  public void setTemporalType(String temporalType) {

  }

    /**
     * Gets valid min.
     *
     * @return the valid min
     */
    public float getValidMin() {
    return 0;
  }

    /**
     * Sets valid min.
     *
     * @param validMin the valid min
     */
    public void setValidMin(float validMin) {

  }

    /**
     * Gets valid max.
     *
     * @return the valid max
     */
    public float getValidMax() {
    return 0;
  }

    /**
     * Sets valid max.
     *
     * @param validMax the valid max
     */
    public void setValidMax(float validMax) {

  }

    /**
     * Gets fill value.
     *
     * @return the fill value
     */
    public float getFillValue() {
    return 0;
  }

    /**
     * Sets fill value.
     *
     * @param fillValue the fill value
     */
    public void setFillValue(float fillValue) {

  }

    /**
     * Sets dataset name.
     *
     * @param datasetName the dataset name
     */
    public void setDatasetName(String datasetName) {

  }

    /**
     * Gets dataset name.
     *
     * @return the dataset name
     */
    public String getDatasetName() {
    return null;
  }

    /**
     * Sets file extension.
     *
     * @param fileExtension the file extension
     */
    public void setFileExtension(String fileExtension) {

  }

    /**
     * Gets file extension.
     *
     * @return the file extension
     */
    public String getFileExtension() {
    return null;
  }

    /**
     * Sets hibernate config xml file.
     *
     * @param hibernateConfigXmlFile the hibernate config xml file
     */
    public void setHibernateConfigXmlFile(String hibernateConfigXmlFile) {
    this.hibernateConfigXmlFile = hibernateConfigXmlFile;
  }

    /**
     * Gets hibernate config xml file.
     *
     * @return the hibernate config xml file
     */
    public String getHibernateConfigXmlFile() {
    return this.hibernateConfigXmlFile;
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
     * Sets geometry id.
     *
     * @param geometryID the geometry id
     */
    public void setGeometryID(int geometryID) {
    this.geometryID = geometryID;
  }

  public String getVariableName() {
    return this.varShortName;
  }

  public void setVariableName(String variableName) {
    this.varShortName = variableName;
  }

  public int getTemporalComponent() {
    return this.temporalComponent;
  }

  public void setTemporalComponent(int temporalComponent) {
    this.temporalComponent = temporalComponent;
  }

  public String getBlockHosts() {
    return this.blockHosts;
  }

  public void setBlockHosts(String blockHosts) {
    this.blockHosts = blockHosts;
  }

  public int getCompressionCode() {
    return this.filterMask;
  }

  public void setCompressionCode(int compressionCode) {
    this.filterMask = compressionCode;
  }

  public void setByteOffset(long byteOffset) {
    this.filePos = byteOffset;
  }

  public long getByteOffset() {
    return this.filePos;
  }

  public void setByteLength(long byteLength) {
    this.byteSize = byteLength;
  }

  public long getByteLength() {
    return this.byteSize;
  }

    /**
     * Gets geometry id.
     *
     * @return the geometry id
     */
    public int getGeometryID() {
    return geometryID;
  }

    /**
     * Instantiates a new Merra 2 variable attribute.
     */
    public Merra2VariableAttribute() {

  }
}
