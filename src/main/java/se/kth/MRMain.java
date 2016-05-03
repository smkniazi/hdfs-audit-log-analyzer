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

public class MRMain extends Configured implements Tool {

  public static final Logger LOG = Logger.getLogger(MRMain.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MRMain(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
//    for(int i = 0 ; i < args.length; i++){
//      String arg = args[i];
//      if(arg.compareToIgnoreCase("-webhdfs") == 0){
//       if((i+1) < args.length){
//         conf.set("webhdfs", args[i+1]);
//       }else{
//         throw new IllegalArgumentException("specify webhdfs address");
//       }
//      }
//    }

    Job job = new Job(conf, "HDFS-Stats");
    if(conf.get("oivWebHdfs")!=null){
      LOG.info("oivWebHdfs is set to "+conf.get("oivWebHdfs"));
    } else {
      LOG.info("oivWebHdfs is not set ");
      for(String arg : args){
        LOG.info("Param "+arg);
      }
    }
    job.setJarByClass(this.getClass());
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(HdfsStatKey.class);
    job.setOutputValueClass(IntWritable.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }



}