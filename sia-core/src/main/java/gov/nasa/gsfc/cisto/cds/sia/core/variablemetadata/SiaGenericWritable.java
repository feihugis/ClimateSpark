package gov.nasa.gsfc.cisto.cds.sia.core.variablemetadata;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.util.Arrays;

/**
 * The type Sia generic writable.
 */
public class SiaGenericWritable extends GenericWritable {

    private static Class<? extends Writable>[] CLASSES = null;

    static {
        CLASSES = (Class<? extends Writable>[]) new Class[] {
                MerraVariableAttribute.class,
                Merra2VariableAttribute.class,
                IntWritable.class
        };
    }

    /**
     * Instantiates a new Sia generic writable.
     */
//this empty initialize is required by Hadoop
    public SiaGenericWritable() {
    }

    /**
     * Instantiates a new Sia generic writable.
     *
     * @param instance the instance
     */
    public SiaGenericWritable(Writable instance) {
        set(instance);
    }

    @Override
    protected Class<? extends Writable>[] getTypes() {
        return CLASSES;
    }

    @Override
    public String toString() {
        return "SiaGenericWritable [getTypes()=" + Arrays.toString(getTypes()) + "]";
    }
}
