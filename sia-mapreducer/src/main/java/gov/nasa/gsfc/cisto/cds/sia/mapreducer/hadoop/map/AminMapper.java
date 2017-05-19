package gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.map;

import gov.nasa.gsfc.cisto.cds.sia.core.io.key.ArraySpec;
import gov.nasa.gsfc.cisto.cds.sia.core.io.value.AreaAvgWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;
import ucar.ma2.IndexIterator;

import java.io.IOException;

/**
 * Performs mapping - prepares splits into arrays of data before applying
 * operation
 * 
 * @author gtamkin based on GMU prototype
 *
 */
public class AminMapper extends Mapper<ArraySpec, Array, Text, AreaAvgWritable> {

	private static final Log LOG = LogFactory.getLog(AminMapper.class);
	AreaAvgWritable valueOutputSumNum = new AreaAvgWritable();
	long sTime;
	long eTime;
	long ioTime = 0L;
	long mapTime = 0L;
	long count = 0L;
	private Text keyOutput = new Text();

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
	 * org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	public void map(ArraySpec key, Array value, Mapper<ArraySpec, Array, Text, AreaAvgWritable>.Context context)
			throws IOException, InterruptedException {

		LOG.info("*******************************  AmaxMapper.map(VariableInfo key");

		ArrayFloat ncArray = (ArrayFloat) value;
		IndexIterator itor = ncArray.getIndexIterator();
		Float validMax = key.getValidMax();
		Float validMin = key.getValidMin();
		Float fillValue = key.getFillValue();
		Float sum = 0.0F;
		int num = 0;
		long timerB = System.currentTimeMillis();

		while (itor.hasNext()) {
			this.count += 1L;
			Float tempValue = itor.getFloatNext();
			if ((tempValue.floatValue() <= validMax.floatValue()) && (tempValue.floatValue() >= validMin.floatValue())
					&& (!tempValue.equals(fillValue))) {
				if (tempValue < sum) {
					sum = tempValue;
				}
				num++;
			}
		}
		
		//TODO: remove?
		long timerC = System.currentTimeMillis();
		this.mapTime = (this.mapTime + timerC - timerB);

		try {
			this.valueOutputSumNum.setResult(sum);
			this.valueOutputSumNum.setNum(num);

			this.keyOutput.set(key.getVarName() + " " + key.getDate());
			context.write(this.keyOutput, this.valueOutputSumNum);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.
	 * Mapper.Context)
	 */
	protected void cleanup(Mapper<ArraySpec, Array, Text, AreaAvgWritable>.Context context)
			throws IOException, InterruptedException {
		this.eTime = System.currentTimeMillis();
		this.ioTime = (this.ioTime + this.eTime - this.sTime - this.mapTime);

		LOG.info("*******************************   FS-io read " + this.count + " values : "
				+ this.ioTime * 1.0D / 1000.0D + " seconds;" + " speed: "
				+ this.count * 4.0D / 1024.0D / 1024.0D / (this.ioTime / 1000L) + "MB/S");
		LOG.info("*******************************   Mapper takes =====" + this.mapTime * 1.0D / 1000.0D + " seconds");
	}
}