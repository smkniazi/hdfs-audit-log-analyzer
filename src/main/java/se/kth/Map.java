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

public class Map extends Mapper<LongWritable, Text, HdfsStatKey, IntWritable> {
  private final static IntWritable one = new IntWritable(1);

  static FileSystem client = null;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = context.getConfiguration();
      String webHdfsURI = conf.get("oivWebHdfs");
      if (webHdfsURI != null) {
        MRMain.LOG.info("WebHdfs is set to "+webHdfsURI);
        conf.set("dfs.http.client.retry.policy.enabled", "true");
        client = (FileSystem) FileSystem.newInstance(URI.create(webHdfsURI), conf);
      } else {
        MRMain.LOG.info("WebHdfs is  not set" );
        client = (FileSystem) FileSystem.newInstance(conf);
      }
  }

  public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
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

