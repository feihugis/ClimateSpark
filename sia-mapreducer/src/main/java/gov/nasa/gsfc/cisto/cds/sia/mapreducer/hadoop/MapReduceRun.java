package gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop;

import gov.nasa.gsfc.cisto.cds.sia.core.config.ConfigParameterKeywords;
import gov.nasa.gsfc.cisto.cds.sia.core.io.value.AreaAvgWritable;
import gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.combine.AavgCombiner;
import gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.inputformat.SiaInputFormat;
import gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.map.AavgMapper;
import gov.nasa.gsfc.cisto.cds.sia.mapreducer.hadoop.reduce.AavgReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Date;

/**
 * Created by Fei Hu on 11/20/16.
 */
public class MapReduceRun extends Configured implements Tool{

  public int run(String[] strings) throws Exception {
    Configuration conf = getConf();
    conf.set("fs.defaultFS", "file:////");

    Job job = new Job(conf);
    String jobNameString = "SIAMapReducer";


    job.getConfiguration().setStrings(ConfigParameterKeywords.variableNames, "LAI");
    job.setJarByClass(MapReduceRun.class);
    job.setMapperClass(AavgMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(AreaAvgWritable.class);
    job.setCombinerClass(AavgCombiner.class);
    job.setReducerClass(AavgReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);

    job.setInputFormatClass(SiaInputFormat.class);
    SiaInputFormat.addInputPath(job, new Path("/Users/feihu/Documents/Data/Merra2/daily/"));
    FileOutputFormat.setOutputPath(job, new Path("/Users/feihu/Desktop/" + (new Date()).getTime()));

    job.waitForCompletion(true);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MapReduceRun(), args);
    System.exit(res);
  }
}
