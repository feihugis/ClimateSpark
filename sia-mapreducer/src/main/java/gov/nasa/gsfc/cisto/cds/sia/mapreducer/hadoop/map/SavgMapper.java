package gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.map;

import gov.nasa.gsfc.cisto.cds.sia.core.VariableInfo;
import gov.nasa.gsfc.cisto.cds.sia.core.io.value.FloatArrayWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;

import java.io.IOException;
import java.io.InterruptedIOException;

/**
 * Created by mkbowen on 6/2/16.
 */
public class SavgMapper extends Mapper<VariableInfo, Array, Text, FloatArrayWritable> {

    private static final Log LOG = LogFactory.getLog(SavgMapper.class);
    private Text keyOutput = new Text();
    private FloatArrayWritable floatArrayWritable;

    public void map(VariableInfo key, Array value, Mapper<VariableInfo, Array, Text, FloatArrayWritable>.Context context) throws IOException, InterruptedIOException {
        ArrayFloat ncArray = (ArrayFloat) value;


        try {
            this.keyOutput = new Text(key.getStandardName() + " " + key.getTime() % 100);
            this.floatArrayWritable = new FloatArrayWritable((float[]) ncArray.get1DJavaArray(float.class));

            context.write(this.keyOutput, this.floatArrayWritable);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
