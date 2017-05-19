package gov.nasa.gsfc.cisto.cds.sia.indexer;

import gov.nasa.gsfc.cisto.cds.sia.core.config.ConfigParameterKeywords;
import gov.nasa.gsfc.cisto.cds.sia.core.config.SiaConfigurationUtils;
import gov.nasa.gsfc.cisto.cds.sia.core.config.UserProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Custom input format for index builder
 *
 * @author mkbowen
 *
 */
public class IndexInputFormat extends FileInputFormat<Text, Text> {

  /**
   * Allows for fine-tuned control over the number of files each mapper processes
   * The number of files assigned per task is read from the job context, which is
   * initialized through a command line parameter
   *
   * @param jobContext The job context; the parameters are initialized via command line arguments
   *                   in the IndexDriver
   * @return inputSplitList List of <InputSplit> objects - each InputSplit contains the variables
   * to be processed and their host locations
   */
  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    List<InputSplit> inputSplitList = new ArrayList<InputSplit>();
    FileSystem fileSystem = FileSystem.get(jobContext.getConfiguration());
    UserProperties userProperties = (UserProperties) SiaConfigurationUtils.deserializeObject(jobContext.getConfiguration().get(ConfigParameterKeywords.userPropertiesSerialized), UserProperties.class);
    String fileFormat = userProperties.getFileExtension();
    List<FileStatus> fileStatusList = listStatus(jobContext);
    IndexSplit inputSplit = null;
    int numFilesInSplit = 0;

    for (FileStatus file : fileStatusList) {

      if (numFilesInSplit == 0) {
        String[] hosts = fileSystem.getFileBlockLocations(file, 0L, file.getLen())[0].getHosts();
        inputSplit = new IndexSplit(userProperties.getVariableNames(), hosts);
      }
      inputSplit.addFilePath(file.getPath().toString());
      numFilesInSplit++;
      if (numFilesInSplit == userProperties.getFilesPerMapTask()) {
        inputSplitList.add(inputSplit);
        numFilesInSplit = 0;
      }
    }

    if (numFilesInSplit != 0) {
      inputSplitList.add(inputSplit);
    }

    return inputSplitList;
  }

  /**
   * Used to filter out files and recursively build a file list to return so that multiple files
   * can be processed at a time
   *
   * @param jobContext The job context; the parameters are initialized via command line arguments
   *                   in the IndexDriver
   * @return allFiles A filtered list of files to be processed in map tasks
   */
  @Override
  protected List<FileStatus> listStatus(final JobContext jobContext) throws IOException {
    Configuration conf = jobContext.getConfiguration();
    FileSystem fileSystem = FileSystem.get(conf);

    List<Path> fileList = new ArrayList<Path>();
    RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fileSystem.listFiles(getInputPaths(jobContext)[0], true);
    while(fileStatusRemoteIterator.hasNext()){
      LocatedFileStatus fileStatus = fileStatusRemoteIterator.next();
      fileList.add(fileStatus.getPath());
    }
    Path[] filePathArray = fileList.toArray(new Path[fileList.size()]);

    PathFilter filter = new PathFilter() {
      public boolean accept(Path path) {
        UserProperties userProperties = (UserProperties) SiaConfigurationUtils.deserializeObject(jobContext.getConfiguration().get(ConfigParameterKeywords.userPropertiesSerialized), UserProperties.class);
        return path.getName().endsWith(userProperties.getFileExtension());
      }
    };

    FileStatus[] filteredFileArray = fileSystem.listStatus(filePathArray, filter);

    return new ArrayList<FileStatus>(Arrays.asList(filteredFileArray));
  }

  /**
   * Generates records, or <K, V> pairs, to be processed by the mapper
   *
   * @param inputSplit object containing the variables to be processed and file host locations
   * @param context Task Tracker context
   */
  @Override
  public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {

    RecordReader recordReader = new IndexRecordReader();

    recordReader.initialize(inputSplit, context);

    return recordReader;
  }

}