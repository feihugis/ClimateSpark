package gov.nasa.gsfc.cisto.cds.sia.core.preprocessing;

import gov.nasa.gsfc.cisto.cds.sia.core.common.FileUtils;
import gov.nasa.gsfc.cisto.cds.sia.core.randomaccessfile.MerraRandomAccessFile;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import ucar.ma2.DataType;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Fei Hu on 9/22/16.
 */
public class VarMeta {
  private static final Log LOG = LogFactory.getLog(VarMeta.class);

  private String varShortName = "";
  private List<VarAttribute> attributeList = new ArrayList<VarAttribute>();

    /**
     * Instantiates a new Var meta.
     *
     * @param varShortName  the var short name
     * @param attributeList the attribute list
     */
    public VarMeta(String varShortName, List<VarAttribute> attributeList) {
    this.varShortName = varShortName;
    this.attributeList = attributeList;
  }

    /**
     * Gets attribute list.
     *
     * @return the attribute list
     */
    public List<VarAttribute> getAttributeList() {
    return attributeList;
  }

    /**
     * Sets attribute list.
     *
     * @param attributeList the attribute list
     */
    public void setAttributeList(
      List<VarAttribute> attributeList) {
    this.attributeList = attributeList;
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
     * Initialize list.
     *
     * @param file the file
     * @return the list
     * @throws IOException the io exception
     */
    public static List<VarMeta> initialize(String file) throws IOException {
    FileSystem fileSystem = FileSystem.get(new Configuration());
    FileStatus fileStatus = fileSystem.getFileStatus(new Path(file));

    MerraRandomAccessFile randomAccessFile = new MerraRandomAccessFile(fileStatus, new Configuration());
    NetcdfFile netCdfFile = NetcdfFile.open(randomAccessFile, fileStatus.getPath().toString());

    List<Variable> varList = netCdfFile.getVariables();
    List<VarMeta> varMetaList = new ArrayList<VarMeta>();

    for (Variable var : varList) {
      List<VarAttribute> varAttributes = new ArrayList<VarAttribute>();
      for (Attribute attr : var.getAttributes()) {
        varAttributes.add(new VarAttribute(attr.getName(), attr.getValues().toString(), attr.getDataType().toString()));
      }
      varAttributes.add(new VarAttribute("dims", var.getDimensionsString(), DataType.STRING.toString()));
      varAttributes.add(new VarAttribute("shape", Arrays.toString(var.getShape()).replaceAll("\\[|\\]|,", ""), DataType.INT.toString()));

      varMetaList.add(new VarMeta(var.getShortName(), varAttributes));
    }
    return varMetaList;
  }

    /**
     * Gets attribute.
     *
     * @param attributeName the attribute name
     * @return the attribute
     */
    public VarAttribute getAttribute(String attributeName) {
    for (VarAttribute varAttribute : attributeList) {
      if (varAttribute.getName().equalsIgnoreCase(attributeName)) {
        return varAttribute;
      }
    }
    LOG.error("Do not find the attribute according to the specified attribute name: " + attributeName);
    return null;
  }

    /**
     * Has attribute boolean.
     *
     * @param attributeName the attribute name
     * @return the boolean
     */
    public boolean hasAttribute(String attributeName) {
    for (VarAttribute varAttribute : attributeList) {
      if (varAttribute.getName().equalsIgnoreCase(attributeName)) {
        return true;
      }
    }
    return false;
  }

    /**
     * Get shape int [ ].
     *
     * @return the int [ ]
     */
    public int[] getShape() {
    String[] shp = null;
    if (this.hasAttribute("_ChunkSize")) {
       shp = this.getAttribute("_ChunkSize").getValue().split(" ");
    } else {
      shp = this.getAttribute("shape").getValue().split(" ");
    }

    int[] shape = FileUtils.stringArrayToIntArray(shp);
    return shape;
  }

    /**
     * Get dims string [ ].
     *
     * @return the string [ ]
     */
    public String[] getDims() {
    return this.getAttribute("dims").getValue().split(" ");
  }

  public String toString() {
    String v = " varShortName = " + this.varShortName + " : ";
    for (VarAttribute varAttribute : attributeList) {
      v = v + varAttribute + " \n";
    }
    return v;
  }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws IOException the io exception
     */
    public static void main(String[] args) throws IOException {
    String file = "/Users/feihu/Documents/Data/Merra2/daily/MERRA2_100.inst1_2d_int_Nx.19800103.nc4";
    VarMeta.initialize(file);
  }
}
