package gov.nasa.gsfc.cisto.cds.sia.core.io.value;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * The type Area avg writable.
 */
public class AreaAvgWritable
    implements Writable {

  private String varName;
  private Float result;
  private int num;

    /**
     * Instantiates a new Area avg writable.
     *
     * @param varName the var name
     * @param result  the result
     * @param num     the num
     */
    public AreaAvgWritable(String varName, Float result, int num) {
    this.varName = varName;
    this.result = result;
    this.num = num;
  }

    /**
     * Instantiates a new Area avg writable.
     *
     * @param result the result
     * @param num    the num
     */
    public AreaAvgWritable(Float result, int num) {
    this.result = result;
    this.num = num;
  }


    /**
     * Instantiates a new Area avg writable.
     */
    public AreaAvgWritable() {
  }

    /**
     * Read area avg writable.
     *
     * @param in the in
     * @return the area avg writable
     * @throws IOException the io exception
     */
    public static AreaAvgWritable read(DataInput in) throws IOException {
    AreaAvgWritable sm = new AreaAvgWritable();
    sm.readFields(in);
    return sm;
  }

  public String toString() {
    return this.varName + this.result + this.num;
  }

  public void write(DataOutput out)
      throws IOException {
    out.writeFloat(this.result.floatValue());
    out.writeInt(this.num);
  }

  public void readFields(DataInput in)
      throws IOException {
    this.result = in.readFloat();
    this.num = in.readInt();
  }

    /**
     * Gets result.
     *
     * @return the result
     */
    public Float getResult() {
    return this.result;
  }

    /**
     * Sets result.
     *
     * @param result the result
     */
    public void setResult(Float result) {
    this.result = result;
  }

    /**
     * Gets num.
     *
     * @return the num
     */
    public int getNum() {
    return this.num;
  }

    /**
     * Sets num.
     *
     * @param num the num
     */
    public void setNum(int num) {
    this.num = num;
  }
}