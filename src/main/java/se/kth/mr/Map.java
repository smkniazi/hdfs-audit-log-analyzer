package se.kth.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import se.kth.mr.tools.HdfsExecutedOperation;
import se.kth.mr.tools.LogLineParser;
import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
  private final static IntWritable one = new IntWritable(1);
  public void map(LongWritable offset, Text lineText, Context context)
          throws IOException, InterruptedException {
    String line = lineText.toString();
    HdfsExecutedOperation executedOperation = LogLineParser.parseHdfsAuditLogLine(line);
    if(executedOperation != null){
      context.write(new Text(executedOperation.getOpName().toString()), one);
      context.
    }
  }
}

