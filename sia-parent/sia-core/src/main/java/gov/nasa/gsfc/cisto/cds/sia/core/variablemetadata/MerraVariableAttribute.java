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
 * The type Merra variable attribute.
 */
public class MerraVariableAttribute implements SiaVariableAttribute, WritableComparable {

    private String variableName;
    private String temporalType;
    private float validMin = -1.0E30F;
    private float validMax = -1.0E30F;
    private float fillValue = 9.9999999E14F;
    private int temporalComponent;
    private String blockHosts;
    private long byteOffset;
    private long byteLength;
    private int compressionCode;
    private int[] corner;

    /**
     * Instantiates a new Merra variable attribute.
     */
    public MerraVariableAttribute() {
    }

    public String getVariableName() {
        return variableName;
    }

    public void setVariableName(String variableName) {
        this.variableName = variableName;
    }

    public int getTemporalComponent() {
        return temporalComponent;
    }

    public void setTemporalComponent(int temporalComponent) {
        this.temporalComponent = temporalComponent;
    }

    public String getBlockHosts() {
        return blockHosts;
    }

    public void setBlockHosts(String blockHosts) {
        this.blockHosts = blockHosts;
    }

    public int getCompressionCode() {
        return compressionCode;
    }

    public void setCompressionCode(int compressionCode) {
        this.compressionCode = compressionCode;
    }

    public String getTemporalType() {
        return temporalType;
    }

    public void setTemporalType(String temporalType) {
        this.temporalType = temporalType;
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

    public long getByteOffset() {
        return byteOffset;
    }

    public void setByteOffset(long byteOffset) {
        this.byteOffset = byteOffset;
    }

    public long getByteLength() {
        return byteLength;
    }

    public int[] getCorner() {
      return this.corner;
    }

    public void setCorner(int[] corner) {
      this.corner = corner;
    }

  public void setByteLength(long byteLength) {
        this.byteLength = byteLength;
    }



    public void readFields(DataInput dataIn) throws IOException {
        this.variableName = (Text.readString(dataIn));
        this.validMax = dataIn.readFloat();
        this.validMin = dataIn.readFloat();
        this.fillValue = dataIn.readFloat();
        this.temporalComponent = dataIn.readInt();
        this.byteOffset = dataIn.readLong();
        this.byteLength = dataIn.readLong();
        this.blockHosts = (Text.readString(dataIn));
        this.temporalType = Text.readString(dataIn);
        int cornerNum = dataIn.readInt();
        this.corner = new int[cornerNum];
        for (int i = 0; i < cornerNum; i++) {
          this.corner[i] = dataIn.readInt();
        }
    }

    public void write(DataOutput dataOut) throws IOException {
        Text.writeString(dataOut, this.variableName);
        dataOut.writeFloat(this.validMax);
        dataOut.writeFloat(this.validMin);
        dataOut.writeFloat(this.fillValue);
        dataOut.writeInt(this.temporalComponent);
        dataOut.writeLong(this.byteOffset);
        dataOut.writeLong(this.byteLength);
        Text.writeString(dataOut, this.blockHosts);
        Text.writeString(dataOut, this.temporalType);
        dataOut.writeInt(this.corner.length);
        for (int location : this.corner) {
          dataOut.writeInt(location);
        }
    }

    public int compareTo(Object object) {
        MerraVariableAttribute merraVariableMetadata = (MerraVariableAttribute) object;

        return ((!this.blockHosts.equals(merraVariableMetadata.getBlockHosts())
                || !this.variableName.equals(merraVariableMetadata.getVariableName()) || this.temporalComponent != merraVariableMetadata.getTemporalComponent()
                || this.byteOffset != merraVariableMetadata.getByteOffset() || this.byteLength != merraVariableMetadata.getByteLength())
                || Arrays.toString(this.corner).equals(Arrays.toString(merraVariableMetadata.corner))? 1 : 0);
    }

    public List<SiaVariableAttribute> getVariableMetadataList(FileSystem fileSystem, String filePath, String[] variablesToBeIndexed) {
        List<SiaVariableAttribute> siaVariableAttributeList = new ArrayList<SiaVariableAttribute>();

        try {
            // FileStatus object -
            // isDirectory, length, replication, blocksize, modification_time, access_time, owner, group, permission, isSymlink
            FileStatus fileStatus = fileSystem.getFileStatus(new Path(filePath));

            // RandomAccessFile object -
            // just file path?
            MerraRandomAccessFile randomAccessFile = new MerraRandomAccessFile(fileStatus, new Configuration());

            // NetcdfFile object -
            // ALL of the netcdf file header data, list of variables, dimensions, etc...
            NetcdfFile netCdfFile = NetcdfFile.open(randomAccessFile, fileStatus.getPath().toString());

            // List of the variables contained in the netcdf file
            List<Variable> varListFromFile = netCdfFile.getVariables();

            for(Variable variableFromFile : varListFromFile) {
                for (String aVarsToBeIndexed : variablesToBeIndexed) {
                    String variableToBeIndexed = aVarsToBeIndexed.replaceAll("\\s", "");

                    if ((variableFromFile.getShortName().equals(variableToBeIndexed)) || (variableFromFile.getFullName().equals(variableToBeIndexed))) {
                        //getVarLocationInformation returns something like:
                        //0:0,0:0,0:360,0:539 start at 8894171, length is 153067; 0:0,1:1,0:360,0:539 start at 9051400, length is 215556; ...
                        //first-dim,second-dim,third-dim,fourth-dim byte-offset, byte-length;...
                        //the byte-offset and length are the offset and length into the file for the variable
                        String variableFromFileLocationInfo = variableFromFile.getVarLocationInformation();
                        String[] variableFromFileLocInfo = variableFromFileLocationInfo.split("; ");

                        for (String aVariableFromFileLocInfo : variableFromFileLocInfo) {
                            String date = getDate(filePath, 2);
                            SiaVariableAttribute siaVariableAttribute = variableMapper(
                                    variableToBeIndexed,
                                    fileSystem,
                                    fileStatus,
                                    aVariableFromFileLocInfo,
                                    date);
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

    public SiaVariableAttribute variableMapper(String variableName, FileSystem fileSystem, FileStatus fileStatus, String variableLocationInfo, String date)
            throws IOException, InvalidRangeException, NullPointerException {
        //split apart the varLocInfo to find extract the offset and length
        String[] unparsedVarOffsetInfo = variableLocationInfo.split(", ")[0].split(" ");
        String[] unparsedVarLengthInfo = variableLocationInfo.split(", ")[1].split(" ");

        long offset = Long.parseLong(unparsedVarOffsetInfo[(unparsedVarOffsetInfo.length - 1)]);
        long len = Long.parseLong(unparsedVarLengthInfo[(unparsedVarLengthInfo.length - 1)]);

        //returns offset,length,host of where the file is in the file system
        BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileStatus, offset, len);
        StringBuilder blockHosts = new StringBuilder();

        for (BlockLocation blockLocation : blockLocations) {
            String[] hosts = blockLocation.getHosts();

            for (int i = 0; i < hosts.length; i++) {
                blockHosts.append(hosts[i]);
                if (i + 1 < hosts.length) {
                    blockHosts.append(",");
                } else {
                    blockHosts.append(";");
                }
            }
        }

        //String dimensionsNoSpaces = date + getPressureLevelFromDimensions(unparsedVarOffsetInfo[0]);

        String[] dimensionInfo = unparsedVarOffsetInfo[0].split(",");
        int[] cornerInfo = new int[dimensionInfo.length];
        for (int i = 0; i < dimensionInfo.length; i++) {
          cornerInfo[i] = Integer.parseInt(dimensionInfo[i].split(":")[0]);
        }

        SiaVariableAttribute siaVariableAttribute = new MerraVariableAttribute();
        siaVariableAttribute.setVariableName(variableName);
        //siaVariableAttribute.setTemporalComponent(Integer.parseInt(dimensionsNoSpaces));
        siaVariableAttribute.setTemporalComponent(Integer.parseInt(date));
        siaVariableAttribute.setCorner(cornerInfo);

        siaVariableAttribute.setByteOffset(offset);
        siaVariableAttribute.setByteLength(len);
        siaVariableAttribute.setBlockHosts(blockHosts.toString());
        siaVariableAttribute.setTemporalType(date);

        return siaVariableAttribute;
    }

    /**
     * Gets date.
     *
     * @param filePath     the filePath of the current file being processed
     * @param datePosition the date position
     * @return the parsed Date string
     */
    public static String getDate(String filePath, int datePosition) {
        String[] tmp = filePath.split("\\.");
        return tmp[tmp.length - datePosition];
    }

    /**
     * Gets pressure level from dimensions.
     *
     * @param dimensions the dimensions
     * @return parsed dimensions associated with a Variable
     */
    public static String getPressureLevelFromDimensions(String dimensions) {
        String[] dimList = dimensions.split(",");
        int threeDimensionalVariableSize = 4;
        int pressureLevelOffset = 3;
        int numberOfDimensions = dimList.length;
        String pressureLevel = "";

        //if dimensions list has 3 components, then there are no pressure levels, variable is 2d, so we only handle dimension lists with 4 components here
        if (numberOfDimensions == threeDimensionalVariableSize) {
            pressureLevel = dimList[numberOfDimensions - pressureLevelOffset].split(":")[0];
            //pad values less than 10 with a zero for uniform database lookups
            if (Integer.parseInt(pressureLevel) < 10) {
                pressureLevel = "0" + pressureLevel;
            }
        }

        return pressureLevel;
    }

    public String toString() {
        return "\nMerraVariableAttribute: " + "temporalType [" + this.temporalType + "] " + "variableName [" + this.variableName + "] "
                + "validMax [" + this.validMax + "] " + "validMin [" + this.validMin + "] "
                + "fillValue [" + this.fillValue + "] " + "temporalComponent [" + this.temporalComponent + "] " + "byteOffset [" + this.byteOffset + "] "
                + "byteLength [" + this.byteLength + "] " + "blockHosts [" + this.blockHosts + "]"
                + "corner [" + Arrays.toString(this.corner) + "]";
    }

  public static void main(String[] args) throws IOException {
    Configuration configuration = new Configuration();
    FileSystem fs = FileSystem.get(configuration);
    MerraVariableAttribute merraVariableAttribute = new MerraVariableAttribute();
    String filePath = "/Users/feihu/Documents/Data/modis_hdf/MOD08_D3/MOD08_D3.A2016001.006.2016008061022.hdf";
    List<SiaVariableAttribute> siaVariableAttributeList = merraVariableAttribute.getVariableMetadataList(fs, filePath, new String[]{"Cov_U_QV", "Cov_U_T"});
    for (SiaVariableAttribute siaVariableAttribute : siaVariableAttributeList) {
      System.out.println(siaVariableAttribute.toString());
    }
  }
}
