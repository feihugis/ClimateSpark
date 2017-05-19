package gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.combine;

import gov.nasa.gsfc.cisto.cds.sia.core.io.value.FloatArrayWritable;
import gov.nasa.gsfc.cisto.cds.sia.core.io.value.FloatMatrixWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by mkbowen on 6/2/16.
 */
public class SavgCombiner extends Reducer<Text, FloatArrayWritable, Text, FloatMatrixWritable> {

    private static final Log LOG = LogFactory.getLog(SavgCombiner.class);

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    public void reduce(Text key, FloatArrayWritable value, Reducer<Text, FloatArrayWritable, Text, FloatArrayWritable>.Context context)
            throws IOException, InterruptedException {

        LOG.info("Combiner key name " + key);
        context.write(key, value);
    }
}