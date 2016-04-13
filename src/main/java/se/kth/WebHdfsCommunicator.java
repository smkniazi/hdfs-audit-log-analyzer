package se.kth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Callable;

/**
 * Created by salman on 4/12/16.
 */
public class WebHdfsCommunicator implements Callable {
  private final HdfsAuditLogsDirReader hdfsAuditLogsReader;
  private final HdfsStats hdfsStats;
  private final FileSystem client;
  private long lastStatusUpdate = 0;
  public WebHdfsCommunicator(String webHdfsURI, HdfsAuditLogsDirReader hdfsAuditLogsReader, HdfsStats hdfsStats) throws
      IOException {
    this.hdfsAuditLogsReader = hdfsAuditLogsReader;
    this.hdfsStats = hdfsStats;
    Configuration conf = new Configuration();
    client = (FileSystem) FileSystem.newInstance(URI.create(webHdfsURI), conf);
  }

  /**
   * Computes a result, or throws an exception if unable to do so.
   *
   * @return computed result
   * @throws Exception if unable to compute a result
   */
  public Object call() throws Exception {
    HdfsExecutedOperation operation = null;
    do {
      try {
        operation = hdfsAuditLogsReader.getHdfsOperation();
        if(operation != null){
//          System.out.println("Thread ID: "+Thread.currentThread().getId()+" "+operation);
          FileStatus fileStatus = client.getFileStatus(new Path(operation.getSrc()));
          HdfsOperation.HdfsOperationType opType = HdfsOperation.HdfsOperationType.FileOp;
          if(fileStatus.isDirectory()){
            opType = HdfsOperation.HdfsOperationType.DirOp;
          }
          hdfsStats.increment(operation.getOpName(), opType, fileStatus.getLen());
          Progress.incrementSuccesfullOps();
          printProgress();
        }
      } catch (Exception e) {
        System.out.println("Unable to verify the operation. Cmd: "+operation.getOpName()+ " Path: "+operation.getSrc
            ()+" Exception: "+e);
        hdfsStats.increment(operation.getOpName(), HdfsOperation.HdfsOperationType.UnDetermined, 0L);
        Progress.incrementFailedOps();
      }
    } while (operation != null);
    return null;
  }

  void printProgress() throws IOException {
    long time = System.currentTimeMillis() - lastStatusUpdate;
    if(time > Progress.PROGRESS_DURATION){
      Progress.printPrgress();
      lastStatusUpdate = System.currentTimeMillis();
    }

  }
}
