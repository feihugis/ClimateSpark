package gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.io;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Fei Hu on 9/8/16.
 */
public class ArrayFloatSerializer extends ArraySerializer {
  ArrayFloat array;

  public ArrayFloatSerializer() {

  }

  public ArrayFloatSerializer(int[] shape, float[] data) {
    array = (ArrayFloat) Array.factory(float.class, shape, data);
  }

  public ArrayFloatSerializer(ArrayFloat array) {
    this.array = array;
  }

  public ArrayFloat getArray() {
    return array;
  }

  @Override
  public void setArray(Array array) {
    this.array = (ArrayFloat) array;
  }

  public void setArray(ArrayFloat array) {
    this.array = array;
  }

  public void write(DataOutput out) throws IOException {
    float[] storage = (float[]) array.getStorage();
    int[] shape = array.getShape();

    out.writeInt(shape.length);
    for (int i=0; i<shape.length; i++) {
      out.writeInt(shape[i]);
    }

    out.writeInt(storage.length);
    for (int i = 0; i<storage.length; i++) {
      out.writeFloat(storage[i]);
    }
  }

  public void readFields(DataInput in) throws IOException {
    int shapeSize = in.readInt();
    int[] shape = new int[shapeSize];
    for (int i=0; i<shapeSize; i++) {
      shape[i] = in.readInt();
    }
    int dataSize = in.readInt();
    float[] data = new float[dataSize];
    for (int i=0; i<dataSize; i++) {
      data[i] = in.readFloat();
    }

    array = (ArrayFloat) Array.factory(float.class, shape, data);
  }

  public void write(Kryo kryo, Output output) {
    float[] storage = (float[]) array.getStorage();
    int[] shape = array.getShape();

    output.writeInt(shape.length);
    for (int i=0; i<shape.length; i++) {
      output.writeInt(shape[i]);
    }

    output.writeInt(storage.length);
    for (int i = 0; i<storage.length; i++) {
      output.writeFloat(storage[i]);
    }
  }


  public void read(Kryo kryo, Input input) {
    int shapeSize = input.readInt();
    int[] shape = new int[shapeSize];
    for (int i=0; i<shapeSize; i++) {
      shape[i] = input.readInt();
    }
    int dataSize = input.readInt();
    float[] data = new float[dataSize];
    for (int i=0; i<dataSize; i++) {
      data[i] = input.readFloat();
    }
    array = (ArrayFloat) Array.factory(float.class, shape, data);
  }

}
