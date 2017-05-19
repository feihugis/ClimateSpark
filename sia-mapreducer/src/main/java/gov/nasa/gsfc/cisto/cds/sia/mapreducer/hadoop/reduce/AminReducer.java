package gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.reduce;

import gov.nasa.gsfc.cisto.cds.sia.core.io.value.AreaAvgWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Aggregates the Mapper outputs and performs the requested operation (e.g., avg, min, max, anomaly, etc.)
 * 
 * @author gtamkin based on GMU prototype
 *
 */
public class AminReducer extends Reducer<Text, AreaAvgWritable, Text, FloatWritable> {

	private FloatWritable minimum = new FloatWritable();
	private Text outputKey = new Text();

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	public void reduce(Text key, Iterable<AreaAvgWritable> value, Reducer<Text, AreaAvgWritable, Text, FloatWritable>.Context context)
			throws IOException, InterruptedException {

		Iterator<AreaAvgWritable> sumNums = value.iterator();
		Float sum = 0.0F;

		while (sumNums.hasNext()) {
			AreaAvgWritable sumNum = sumNums.next();
			sum = sum.floatValue() + sumNum.getResult().floatValue();
		}

		this.minimum.set(sum.floatValue());
		String[] name = key.toString().split("/");
		this.outputKey.set(name[(name.length - 1)] + "  minimum:");
		context.write(this.outputKey, this.minimum);
	}
}