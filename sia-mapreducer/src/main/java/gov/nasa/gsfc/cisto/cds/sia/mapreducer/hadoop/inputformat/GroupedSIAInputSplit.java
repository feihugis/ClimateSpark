package gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.inputformat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Fei Hu on 3/12/18.
 */
public class GroupedSIAInputSplit extends InputSplit implements Writable {

  private List<SiaInputSplit> siaInputSplitList;
  private String[] hosts;

  public GroupedSIAInputSplit() {

  }

  public GroupedSIAInputSplit(List<SiaInputSplit> siaInputSplits, String[] hosts) {
    this.siaInputSplitList = siaInputSplits;
    this.hosts = hosts;
  }

  public List<SiaInputSplit> getSiaInputSplitList() {
    return siaInputSplitList;
  }

  public String[] getHosts() {
    return hosts;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return siaInputSplitList.size();
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return hosts;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(siaInputSplitList.size());
    for (SiaInputSplit siaInputSplit : siaInputSplitList) {
      siaInputSplit.write(out);
    }

    out.writeInt(hosts.length);
    for (String host : hosts) {
      Text.writeString(out, host);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int siaInputSplitNum = in.readInt();
    siaInputSplitList = new ArrayList<SiaInputSplit>();

    for (int i = 0; i < siaInputSplitNum; i++) {
      SiaInputSplit siaInputSplit = new SiaInputSplit();
      siaInputSplit.readFields(in);
      this.siaInputSplitList.add(siaInputSplit);
    }

    int hostNum = in.readInt();
    hosts = new String[hostNum];

    for (int i = 0; i < hostNum; i++) {
      this.hosts[i] = Text.readString(in);
    }
  }
}
