package gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.inputformat;

import gov.nasa.gsfc.cisto.cds.sia.core.config.ConfigParameterKeywords;
import gov.nasa.gsfc.cisto.cds.sia.core.config.SiaConfigurationUtils;
import gov.nasa.gsfc.cisto.cds.sia.core.config.SpatiotemporalFilters;
import gov.nasa.gsfc.cisto.cds.sia.core.io.SiaChunk;
import gov.nasa.gsfc.cisto.cds.sia.core.io.key.VarKey;
import gov.nasa.gsfc.cisto.cds.sia.core.io.reader.SIAIOProvider;
import gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.io.ArraySerializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Fei Hu on 9/12/16.
 */
public class SiaRecordReader extends RecordReader<VarKey, ArraySerializer> {
  private static final Log LOG = LogFactory.getLog(SiaRecordReader.class);

  private List<SiaChunk> siaChunkList = new ArrayList<SiaChunk>();
  private int currentKeyMark = -1;
  private int keySize = 0;
  private boolean debug = false;
  private FSDataInputStream inputStream;
  private Configuration conf;
  private int[] queryCorner = null;
  private int[] queryShape = null;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    SiaInputSplit siaInputSplit = (SiaInputSplit) inputSplit;
    siaChunkList = siaInputSplit.getSiaChunkList();
    keySize = siaChunkList.size();
    conf = taskAttemptContext.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(siaChunkList.get(0).getFilePath());
    inputStream = fs.open(path);
    initializeQueryBounding(conf);
  }

  private void initializeQueryBounding(Configuration conf) {
    SpatiotemporalFilters spatiotemporalFilters = (SpatiotemporalFilters) SiaConfigurationUtils.
        deserializeObject(conf.get(ConfigParameterKeywords.spatiotemporalFiltersSerialized),
                          SpatiotemporalFilters.class);
    queryCorner = spatiotemporalFilters.getStartSpatialBounding();
    queryShape = new int[queryCorner.length];
    int[] endCorner = spatiotemporalFilters.getEndSpatialBounding();
    for (int i = 0; i < endCorner.length; i++) {
      queryShape[i] = endCorner[i] - queryCorner[i] + 1;
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    currentKeyMark++;
    return currentKeyMark < siaChunkList.size();
  }

  @Override
  public VarKey getCurrentKey() throws IOException, InterruptedException {
    SiaChunk dataChunk = siaChunkList.get(currentKeyMark);

    int[] targetCorner = new int[queryCorner.length], targetShape = new int[queryCorner.length];
    int[] relativeCorner = new int[targetCorner.length];

    if (queryCorner != null && queryShape != null) {
      if (SiaInputSplitFactory
          .isIntersected(dataChunk.getCorner(), dataChunk.getShape(), queryCorner, queryShape,
                         targetCorner, targetShape)) {
        for (int i = 0; i < relativeCorner.length; i++) {
          relativeCorner[i] = targetCorner[i] - dataChunk.getCorner()[i];
        }
      }
    }

    VarKey varKey = new VarKey("Test", dataChunk.getVarShortName(), dataChunk.getTime(), relativeCorner, targetShape, dataChunk.getDimensions(),
                               "-10000000000", "10000000000", "99999999999");
    return varKey;
  }

  @Override
  public ArraySerializer getCurrentValue() throws IOException, InterruptedException {
    SiaChunk dataChunk = this.siaChunkList.get(currentKeyMark);
    ArraySerializer value = null;

    int[] targetCorner = new int[queryCorner.length], targetShape = new int[queryCorner.length];

    if (queryCorner != null && queryShape != null) {
      if (SiaInputSplitFactory.isIntersected(dataChunk.getCorner(), dataChunk.getShape(), queryCorner, queryShape, targetCorner, targetShape)) {
        Array array = SIAIOProvider.read(dataChunk, inputStream);
        int[] relativeCorner = new int[targetCorner.length];
        for (int i = 0; i < relativeCorner.length; i++) {
          relativeCorner[i] = targetCorner[i] - dataChunk.getCorner()[i];
        }
        try {
          array = array.section(relativeCorner, targetShape).copy();

          if (array.getShape().length != relativeCorner.length) {
            array = array.reshape(targetShape);
          }
        } catch (InvalidRangeException e) {
          LOG.error(e.toString());
          e.printStackTrace();
        }
        value = ArraySerializer.factory(array);
        return value;
      } else {
        return null;
      }
    } else {
      Array array = SIAIOProvider.read(dataChunk, inputStream);
      value = ArraySerializer.factory(array);
      return value;
    }
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return currentKeyMark/keySize;
  }

  @Override
  public void close() throws IOException {

  }
}
