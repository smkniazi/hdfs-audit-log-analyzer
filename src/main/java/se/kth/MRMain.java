package se.kth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;

import java.io.IOException;


public class MRMain extends Configured implements Tool {

  public static final Logger LOG = Logger.getLogger(MRMain.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MRMain(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();

    for (String arg : args) {
      LOG.info("Params " + arg);
    }
    String inputSplit = conf.get("splitSize");
    if (inputSplit != null) {
      LOG.info("InputSplit is set to " + inputSplit);
      conf.set("mapreduce.input.fileinputformat.split.maxsize", inputSplit);
    }

    String hdfs = conf.get("pullStatsFrom");
    if (hdfs != null) {
      LOG.info("Pull stats from " + hdfs);
    }

    String samplePercent = conf.get("samplePercent");
    if (samplePercent != null) {
      int sp = Integer.parseInt(samplePercent);
      if(sp < 0 || sp > 100){
        String msg = "Invalid data sampling percentage. Only integer values [1,100]";
        LOG.error(msg);
        throw new IOException(msg);
      }
      LOG.info("Sampling Percentage: "+samplePercent);
    }
    Job job = new Job(conf, "HDFS-Stats");
    job.setJarByClass(this.getClass());
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileInputFormat.setInputDirRecursive(job, true);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setInputFormatClass(CombineTextInputFormat.class);
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(HdfsStatKey.class);
    job.setOutputValueClass(IntWritable.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }


}
