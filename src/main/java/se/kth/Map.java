package se.kth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.Random;

public class Map extends Mapper<LongWritable, Text, HdfsStatKey, LongWritable> {
  private final static LongWritable one = new LongWritable(1);

  static FileSystem client = null;
  static int sp = 100;
  static Random rand = new Random(System.currentTimeMillis());

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = context.getConfiguration();
    String hdfsURI = conf.get("pullStatsFrom");
    if (hdfsURI != null) {
      conf.set("dfs.http.client.retry.policy.enabled", "true");
      client = (FileSystem) FileSystem.newInstance(URI.create(hdfsURI), conf);
    } else {
      client = (FileSystem) FileSystem.newInstance(conf);
    }

    String samplePercent = conf.get("samplePercent");
    if (samplePercent != null) {
      sp = Integer.parseInt(samplePercent); // sanity check performed by the main class
    }
  }

  public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
    if (sp != 100) {
      int randValue = rand.nextInt(100) + 1; // +1 because we want to exclude 0 and include 100
      if (randValue > sp) {
            return;
      }
    }

    String line = lineText.toString();
    HdfsExecutedOperation operation = LogLineParser.parseHdfsAuditLogLine(line);
    if (operation != null) {
      String path = null;
      if (operation.getOpName() == ValidHdfsOperations.HdfsOperationName.rename) {
        path = operation.getDst();
      } else {
        path = operation.getSrc();
      }
      if (path == null) {
        return;
      }
      try {
        FileStatus fileStatus = client.getFileStatus(new Path(path));
        ValidHdfsOperations.HdfsOperationType opType = ValidHdfsOperations.HdfsOperationType.FileOp;
        if (fileStatus.isDirectory()) {
          opType = ValidHdfsOperations.HdfsOperationType.DirOp;
        }
        long size = opType == ValidHdfsOperations.HdfsOperationType.DirOp ? -1 : fileStatus.getLen();
        context.write(new HdfsStatKey(size, operation.getOpName(), opType), one);
      } catch (Exception e) {
        context
                .write(new HdfsStatKey(-1, operation.getOpName(), ValidHdfsOperations.HdfsOperationType.UnDetermined), one);
      }
    }
  }
}

