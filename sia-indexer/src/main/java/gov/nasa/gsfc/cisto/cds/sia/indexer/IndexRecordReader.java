package gov.nasa.gsfc.cisto.cds.sia.indexer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Handles splits generated from IndexSplit - takes a split and generates a <K,V> record pair to be
 * processed by the mapper
 *
 * @author mkbowen, feihu
 *
 */
public class IndexRecordReader extends RecordReader<Text, Text> {

  private int count = -1;
  private String variableListString;
  private String[] fileArray;

  @Override
  public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
    IndexSplit indexSplit = ((IndexSplit) arg0);
    List<String> fileList = indexSplit.getFileList();
    //TODO: Maybe we can directly use fileList
    this.fileArray = fileList.toArray(new String[fileList.size()]);
    this.variableListString = Arrays.toString(indexSplit.getVariables()).replace("[", "").replace("]", "");
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return new Text(this.fileArray[this.count]);
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return new Text(this.variableListString);
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return (1.0F * this.count) / this.fileArray.length;
  }

  /**
   * This method, as overwritten, does not read the next <K, V> pair.  Only returns true if another <K, V> exists.
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    this.count++;
    return this.count < this.fileArray.length;
  }	
}
