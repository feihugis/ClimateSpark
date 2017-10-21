package gov.nasa.gsfc.cisto.cds.sia.core.io.value;

import org.apache.hadoop.io.Writable;
import org.jblas.FloatMatrix;
import org.jblas.JavaBlas;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;

/**
 * The type Float matrix writable.
 */
public class FloatMatrixWritable extends FloatMatrix implements Writable {

    /**
     * Instantiates a new Float matrix writable.
     */
    public FloatMatrixWritable() {
        super();
    }

    /**
     * Instantiates a new Float matrix writable.
     *
     * @param rows  the rows
     * @param cols  the cols
     * @param array the array
     */
    public FloatMatrixWritable(int rows, int cols, float... array) {
        super(rows, cols, array);
    }

    /**
     * Instantiates a new Float matrix writable.
     *
     * @param rows the rows
     * @param cols the cols
     */
    public FloatMatrixWritable(int rows, int cols) {
        super(rows, cols);
    }

    /**
     * Instantiates a new Float matrix writable.
     *
     * @param m the m
     */
    public FloatMatrixWritable(FloatMatrix m) {
        super(m.rows, m.columns);
        JavaBlas.rcopy(m.length, m.data, 0, 1, this.data, 0, 1);
    }

    public void write(DataOutput out) throws IOException {
        byte[] array = toByteArray(data);
        out.writeInt(array.length);
        out.writeInt(rows);
        out.writeInt(columns);
        out.write(array);
    }

    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        this.rows = in.readInt();
        this.columns = in.readInt();
        byte[] array = new byte[length];
        in.readFully(array);
        this.data = toFloatArray(array);
        this.length = data.length;
    }

    /**
     * To byte array byte [ ].
     *
     * @param floatArray the float array
     * @return the byte [ ]
     */
    public byte[] toByteArray(float[] floatArray) {
        byte byteArray[] = new byte[floatArray.length*4];
        ByteBuffer byteBuf = ByteBuffer.wrap(byteArray);
        FloatBuffer floatBuf = byteBuf.asFloatBuffer();
        floatBuf.put (floatArray);
        return byteArray;
    }

    /**
     * To float array float [ ].
     *
     * @param byteArray the byte array
     * @return the float [ ]
     */
    public float[] toFloatArray(byte[] byteArray) {
        float floatArray[] = new float[byteArray.length/4];
        ByteBuffer byteBuf = ByteBuffer.wrap(byteArray);
        FloatBuffer floatBuf = byteBuf.asFloatBuffer();
        floatBuf.get (floatArray);
        return floatArray;
    }
}
