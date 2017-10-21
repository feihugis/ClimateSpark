package gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.map;

import gov.nasa.gsfc.cisto.cds.sia.core.io.key.VarKey;
import gov.nasa.gsfc.cisto.cds.sia.core.io.value.AreaAvgWritable;
import gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.io.ArraySerializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ucar.ma2.ArrayFloat;
import ucar.ma2.IndexIterator;

import java.io.IOException;

/**
 * Performs mapping - prepares splits into arrays of data before applying operation
 * 
 * @author gtamkin based on GMU prototype
 *
 */
public class AavgMapper extends Mapper<VarKey, ArraySerializer, Text, AreaAvgWritable> {

	private static final Log LOG = LogFactory.getLog(AavgMapper.class);
	AreaAvgWritable valueOutputSumNum = new AreaAvgWritable();
	long sTime;
	long eTime;
	long ioTime = 0L;
	long mapTime = 0L;
	long count = 0L;
	private Text keyOutput = new Text();


	public void map(VarKey key, ArraySerializer value, Mapper<VarKey, ArraySerializer, Text, AreaAvgWritable>.Context context)
			throws IOException, InterruptedException {
		//LOG.info("*******************************  AavgMapper.map(VariableInfo key");
                if (value == null) return;

		ArrayFloat ncArray = (ArrayFloat) value.getArray();
		IndexIterator iterator = ncArray.getIndexIterator();
		Float validMax = 100000000000000000F;
		Float validMin = -100000000000000000F;
		Float fillValue = -999999999999999F; //key.getmissingDataFillValue();
		Float sum = 0.0F;
		int num = 0;
		long timerB = System.currentTimeMillis();

		System.out.println("********************** ncarraylength " + ncArray.getSize());

		while (iterator.hasNext()) {
			this.count += 1L;
			Float tempValue = iterator.getFloatNext();
			if ((tempValue <= validMax) && (tempValue >= validMin) && (!tempValue.equals(fillValue))) {
				sum = tempValue + sum;
				num++;
			}
		}

		long timerC = System.currentTimeMillis();
		this.mapTime = (this.mapTime + timerC - timerB);

		try {
			this.valueOutputSumNum.setResult(sum);
			this.valueOutputSumNum.setNum(num);

			this.keyOutput.set(key.getVarName() + " " + key.getTime() / 100);
			context.write(this.keyOutput, this.valueOutputSumNum);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}