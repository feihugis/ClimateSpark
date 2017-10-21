package gov.nasa.gsfc.cisto.cds.sia.indexer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Splits files to be processed by mapper
 *
 * @author mkbowen
 *
 */
public class IndexSplit extends InputSplit implements Writable {

  private List<String> fileList = new ArrayList();
  private String[] variableArray;
  private String[] hostArray;

  /**
   *
   */
  public IndexSplit() {

  }

  /**
   *
   * @param vars
   * @param hostArray
   */
  public IndexSplit(String[] variableNames, String[] hostArray) {
    this.variableArray = variableNames;
    this.hostArray = hostArray;
  }

  /**
   *
   * @param fileName
   */
  public void addFilePath(String fileName) { this.fileList.add(fileName);}

  /**
   *
   * @return
   */
  public List<String> getFileList() { return this.fileList; }

  /**
   *
   * @return
   */
  public String[] getVariables() { return this.variableArray; }

  /**
   *
   * @param out
   * @throws IOException
   */
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.fileList.size());
    for (String aFileList : this.fileList) {
      Text.writeString(out, aFileList);
    }

    out.writeInt(this.hostArray.length);
    for (String aHostArray : this.hostArray) {
      Text.writeString(out, aHostArray);
    }

    out.writeInt(this.variableArray.length);
    for (String aVariableArray : this.variableArray) {
      Text.writeString(out, aVariableArray);
    }
  }

  /**
   *
   * @param in
   * @throws IOException
   */
  public void readFields(DataInput in) throws IOException {
    int fileNum = in.readInt();
    this.fileList = new ArrayList();

    for (int i = 0; i < fileNum; i++) {
      this.fileList.add(Text.readString(in));
    }

    int hostNum = in.readInt();
    this.hostArray = new String[hostNum];
    for (int i = 0; i < hostNum; i++) {
      this.hostArray[i] = Text.readString(in);
    }

    int varNum = in.readInt();
    this.variableArray = new String[varNum];
    for (int i = 0; i < varNum; i++) {
      this.variableArray[i] = Text.readString(in);
    }
  }

  /**
   * Get the size of the split, so that the input splits can be sorted by size.
   *
   * @return Size of the split
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public long getLength() throws IOException, InterruptedException {
    return this.fileList.size();
  }

  /**
   * Get the list of nodes by name where the data for the split would be local.
   *
   * @return names of hosts where splits are stored
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return this.hostArray;
  }
}