package gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.inputformat;

import gov.nasa.gsfc.cisto.cds.sia.core.io.SiaChunk;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Fei Hu on 9/12/16.
 */
public class SiaInputSplit extends InputSplit implements Writable {

  private List<SiaChunk> siaChunkList = new ArrayList<SiaChunk>();

  public SiaInputSplit(){
  }

  public SiaInputSplit(List<SiaChunk> siaChunkList) {
    this.siaChunkList = siaChunkList;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return this.siaChunkList.size();
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return siaChunkList.get(0).getHosts();
  }

  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(siaChunkList.size());
    for (SiaChunk entity : siaChunkList) {
      entity.write(dataOutput);
    }
  }

  public void readFields(DataInput dataInput) throws IOException {
    siaChunkList = new ArrayList<SiaChunk>();
    int n = dataInput.readInt();
    while (n > 0) {
      try {
        SiaChunk entity = new SiaChunk();
        entity.readFields(dataInput);
        siaChunkList.add(entity);
        n = n -1;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public List<SiaChunk> getSiaChunkList() {
    return siaChunkList;
  }

  public void setSiaChunkList(
      List<SiaChunk> siaChunkList) {
    this.siaChunkList = siaChunkList;
  }
}
