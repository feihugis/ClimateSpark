package gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.meta;

import gov.nasa.gsfc.cisto.cds.sia.core.randomaccessfile.MerraRandomAccessFile;
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
import java.util.HashMap;
import java.util.List;

/**
 * Created by Fei Hu on 9/22/16.
 */
public class VarMeta {
  private String varShortName = "";
  private HashMap<String, VarAttribute> attributeMap = new HashMap<String, VarAttribute>();
  private List<VarAttribute> attributeList = new ArrayList<VarAttribute>();

  public VarMeta(String varShortName, List<VarAttribute> attributeList) {
    this.varShortName = varShortName;
    this.attributeList = attributeList;

    for (VarAttribute attribute : this.attributeList) {
      attributeMap.put(attribute.getName(), attribute);
    }
  }

  public List<VarAttribute> getAttributeList() {
    return attributeList;
  }

  public void setAttributeList(
      List<VarAttribute> attributeList) {
    this.attributeList = attributeList;
  }

  public String getVarShortName() {
    return varShortName;
  }

  public void setVarShortName(String varShortName) {
    this.varShortName = varShortName;
  }

  public VarAttribute getVarAttribute(String attributeName) {
    return this.attributeMap.get(attributeName);
  }

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

  public String toString() {
    String v = " varShortName = " + this.varShortName + " : ";
    for (VarAttribute varAttribute : attributeList) {
      v = v + varAttribute + " ;\n";
    }
    return v;
  }

  public static void main(String[] args) throws IOException {
    String file = "/Users/feihu/Documents/Data/Merra2/daily/MERRA2_100.inst1_2d_int_Nx.19800103.nc4";
    VarMeta.initialize(file);
  }
}
