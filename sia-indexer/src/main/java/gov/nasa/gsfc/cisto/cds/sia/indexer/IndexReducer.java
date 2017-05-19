package gov.nasa.gsfc.cisto.cds.sia.indexer;

import gov.nasa.gsfc.cisto.cds.sia.core.variablemetadata.SiaGenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Fei Hu on 1/10/17.
 */
public class IndexReducer extends Reducer<Text, SiaGenericWritable, Text, IntWritable> {

  public void reduce(Text tableName, Iterable<SiaGenericWritable> nums,
                     Reducer<Text, SiaGenericWritable, Text, IntWritable>.Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    for (SiaGenericWritable num : nums) {
      int n = ((IntWritable) num.get()).get();
      sum = sum + n;
    }

    context.write(tableName, new IntWritable(sum));
  }

}
