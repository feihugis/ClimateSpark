package gov.nasa.gsfc.cisto.cds.sia.core.io.value;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * The type Float array writable.
 */
public class FloatArrayWritable implements Writable {

  private static final Log LOG = LogFactory.getLog(FloatArrayWritable.class);
  private float[] values;

    /**
     * Instantiates a new Float array writable.
     */
    public FloatArrayWritable() {

  }

    /**
     * Instantiates a new Float array writable.
     *
     * @param input the input
     */
    public FloatArrayWritable(float[] input) {
    this.values = input;
  }

    /**
     * Get values float [ ].
     *
     * @return the float [ ]
     */
    public float[] getValues() {
    return values;
  }

    /**
     * Sets data.
     *
     * @param values the values
     */
    public void setData(float[] values) {
    this.values = values;
  }

  public void write(DataOutput out) throws IOException {
    int length = 0;

    if(values != null) {
      length = values.length;
    }

    out.writeInt(length);

    for(int i = 0; i < length; i++) {
      out.writeFloat(values[i]);
    }
  }

  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    this.values = new float[length];

    for (int i = 0; i < length; i++) {
      this.values[i] = in.readFloat();
    }
  }
}