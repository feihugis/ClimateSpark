package gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.combine;

import gov.nasa.gsfc.cisto.cds.sia.core.io.value.AreaAvgWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Combines Mapper results before distribution to Reducers
 * 
 * @author gtamkin based on GMU prototype
 *
 */
public class AminCombiner extends Reducer<Text, AreaAvgWritable, Text, AreaAvgWritable> {

	private AreaAvgWritable outputValue = new AreaAvgWritable();

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN,
	 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	public void reduce(Text key, Iterable<AreaAvgWritable> value,
			Reducer<Text, AreaAvgWritable, Text, AreaAvgWritable>.Context context)
					throws IOException, InterruptedException {
		Iterator<AreaAvgWritable> sumNums = value.iterator();
		int num = 0;
		Float sum = 0.0F;

		while (sumNums.hasNext()) {
			AreaAvgWritable sumNum = sumNums.next();
			if (sumNum.getResult().floatValue() < sum) {
				sum = sumNum.getResult().floatValue();
			}
			num += sumNum.getNum();
		}

		this.outputValue.setResult(sum);
		this.outputValue.setNum(num);

		context.write(key, new AreaAvgWritable(sum, num));
	}
}