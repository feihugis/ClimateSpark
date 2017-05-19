package gov.nasa.gsfc.cisto.cds.sia.core.preprocessing;

/**
 * Created by Fei Hu on 9/22/16.
 */
public class VarAttribute {
  private String name = "";
  private String value = "";
  private String dataType = "";

    /**
     * Instantiates a new Var attribute.
     *
     * @param name     the name
     * @param value    the value
     * @param dataType the data type
     */
    public VarAttribute(String name, String value, String dataType) {
    this.name = name;
    this.value = value;
    this.dataType = dataType;
  }

    /**
     * Gets name.
     *
     * @return the name
     */
    public String getName() {
    return name;
  }

    /**
     * Sets name.
     *
     * @param name the name
     */
    public void setName(String name) {
    this.name = name;
  }

    /**
     * Gets value.
     *
     * @return the value
     */
    public String getValue() {
    return value;
  }

    /**
     * Sets value.
     *
     * @param value the value
     */
    public void setValue(String value) {
    this.value = value;
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

  @Override
  public String toString() {
    return this.name + " = " + this.value + " ";
  }
}
