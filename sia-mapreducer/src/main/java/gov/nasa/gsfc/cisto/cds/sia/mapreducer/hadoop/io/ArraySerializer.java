package gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.io;

import com.esotericsoftware.kryo.KryoSerializable;
import org.apache.hadoop.io.Writable;
import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;

/**
 * Created by Fei Hu on 9/8/16.
 */
public abstract class ArraySerializer implements KryoSerializable, Writable {

  public abstract Array getArray();
  public abstract void setArray(Array array);

  public static ArraySerializer factory(Array array) {
    if (array instanceof ArrayFloat) {
      ArrayFloatSerializer arrayFloatSerializer = new ArrayFloatSerializer();
      arrayFloatSerializer.setArray(array);
      return arrayFloatSerializer;
    }

    return null;
  }

}
