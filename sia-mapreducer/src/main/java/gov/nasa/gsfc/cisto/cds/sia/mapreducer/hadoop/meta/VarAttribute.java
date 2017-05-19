package gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.meta;

/**
 * Created by Fei Hu on 9/22/16.
 */
public class VarAttribute {
  private String name = "";
  private String value = "";
  private String dataType = "";

  public VarAttribute(String name, String value, String dataType) {
    this.name = name;
    this.value = value;
    this.dataType = dataType;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public String getDataType() {
    return dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  @Override
  public String toString() {
    return this.name + " = " + this.value;
  }
}
