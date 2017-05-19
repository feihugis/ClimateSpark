/** @file VarKey.java
 *  @brief Give me a brief description of what this does
 *
 *  A more detailed description
 *  should go here
 *
 *  @author Full Name (userid)
 *  @author Full Name (userid)
 *  @bug Are there any known bugs, if not put No known bugs
 */

package gov.nasa.gsfc.cisto.cds.sia.core.io.key;

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
 * Created by Fei Hu on 11/28/16.
 */
public class VarKey implements Writable, KryoSerializable {

  private String collectionName;
  private String varName;
  private int[] corner = null;         //relative to the whole picture
  private int[] shape = null;          //chunk shape; to get the endcorner: corner[0] + shape[0] - 1
  private String[] dimensions = null;  //dimension info  for each dimension, such [time, lat, lon]
  private int time;
  private String validMin;
  private String validMax;
  private String fillValue;

    /**
     * Instantiates a new Var key.
     *
     * @param collectionName the collection name
     * @param varName        the var name
     * @param time           the time
     * @param corner         the corner
     * @param shape          the shape
     * @param dimensions     the dimensions
     * @param validMin       the valid min
     * @param validMax       the valid max
     * @param fillValue      the fill value
     * @brief
     */
    public VarKey(String collectionName, String varName, int time, int[] corner, int[] shape,
                String[] dimensions, String validMin, String validMax, String fillValue) {
    this.collectionName = collectionName;
    this.varName = varName;
    this.corner = corner;
    this.shape = shape;
    this.dimensions = dimensions;
    this.validMin = validMin;
    this.validMax = validMax;
    this.fillValue = fillValue;
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

    out.writeString(this.collectionName);
    out.writeString(this.varName);
    out.writeString(this.validMax);
    out.writeString(this.validMin);
    out.writeString(this.fillValue);

    out.writeInt(this.time);
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

    collectionName = in.readString();
    varName = in.readString();
    validMax = in.readString();
    validMin = in.readString();
    fillValue = in.readString();

    time = in.readInt();
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

    Text.writeString(out, collectionName);
    Text.writeString(out, varName);
    Text.writeString(out, validMax);
    Text.writeString(out, validMin);
    Text.writeString(out, fillValue);

    out.writeInt(this.time);
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

    collectionName = Text.readString(in);
    varName = Text.readString(in);
    validMax = Text.readString(in);
    validMin = Text.readString(in);
    fillValue = Text.readString(in);

    time = in.readInt();
  }

    /**
     * Gets collection name.
     *
     * @return the collection name
     */
    public String getCollectionName() {
    return collectionName;
  }

    /**
     * Sets collection name.
     *
     * @param collectionName the collection name
     */
    public void setCollectionName(String collectionName) {
    this.collectionName = collectionName;
  }

    /**
     * Gets var name.
     *
     * @return the var name
     */
    public String getVarName() {
    return varName;
  }

    /**
     * Sets var name.
     *
     * @param varName the var name
     */
    public void setVarName(String varName) {
    this.varName = varName;
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
     * Gets valid min.
     *
     * @return the valid min
     */
    public String getValidMin() {
    return validMin;
  }

    /**
     * Sets valid min.
     *
     * @param validMin the valid min
     */
    public void setValidMin(String validMin) {
    this.validMin = validMin;
  }

    /**
     * Gets valid max.
     *
     * @return the valid max
     */
    public String getValidMax() {
    return validMax;
  }

    /**
     * Sets valid max.
     *
     * @param validMax the valid max
     */
    public void setValidMax(String validMax) {
    this.validMax = validMax;
  }

    /**
     * Gets fill value.
     *
     * @return the fill value
     */
    public String getFillValue() {
    return fillValue;
  }

    /**
     * Sets fill value.
     *
     * @param fillValue the fill value
     */
    public void setFillValue(String fillValue) {
    this.fillValue = fillValue;
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
}
