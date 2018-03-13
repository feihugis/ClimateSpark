package gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.inputformat;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;

import gov.nasa.gsfc.cisto.cds.sia.core.io.key.VarKey;
import gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.io.ArraySerializer;

/**
 * Created by Fei Hu on 3/12/18.
 */
public class GroupedSiaRecordReader extends RecordReader<VarKey, ArraySerializer> {

  private SiaRecordReader siaRecordReader = null;
  private Iterator<SiaInputSplit> siaInputSplitIterator = null;
  private int curKeyMark = -1;
  private TaskAttemptContext taskAttemptContext = null;
  private int count = 0;
  private int totalSplitNum = 1;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    GroupedSIAInputSplit groupedSIAInputSplit = (GroupedSIAInputSplit) split;
    totalSplitNum = groupedSIAInputSplit.getSiaInputSplitList().size();
    siaInputSplitIterator = groupedSIAInputSplit.getSiaInputSplitList().listIterator();
    this.taskAttemptContext = context;
    siaRecordReader = new SiaRecordReader();
    curKeyMark++;
    siaRecordReader.initialize(siaInputSplitIterator.next(), context);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (siaRecordReader.nextKeyValue()) {
      return true;
    } else if (siaInputSplitIterator.hasNext()) {
      count++;
      siaRecordReader.initialize(siaInputSplitIterator.next(), this.taskAttemptContext);
      return nextKeyValue();
    } else {
      return false;
    }
  }

  @Override
  public VarKey getCurrentKey() throws IOException, InterruptedException {
    return siaRecordReader.getCurrentKey();
  }

  @Override
  public ArraySerializer getCurrentValue() throws IOException, InterruptedException {
    return siaRecordReader.getCurrentValue();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return count * 1.0f / totalSplitNum;
  }

  @Override
  public void close() throws IOException {

  }
}
