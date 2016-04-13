package se.kth;

/**
 * Created by salman on 2016-04-12.
 */
public class HdfsStats {
  private final HdfsAllOpsStats allStats;
  private final HdfsFileOpsStats fileStats;
  private final HdfsAllOpsStats undeterminedOpsStats;
  public HdfsStats(){
    this.allStats = new HdfsAllOpsStats();
    this.fileStats = new HdfsFileOpsStats();
    this.undeterminedOpsStats = new HdfsAllOpsStats();
  }

  public synchronized  void increment(HdfsOperation.HdfsOperationName opName, HdfsOperation.HdfsOperationType opType,
      Long size){
    if(opType == HdfsOperation.HdfsOperationType.UnDetermined){
      undeterminedOpsStats.increment(opName, false);
    }else {
      boolean isDirOp = opType == HdfsOperation.HdfsOperationType.DirOp;
      allStats.increment(opName, isDirOp);
      if (opType == HdfsOperation.HdfsOperationType.FileOp) {
        fileStats.increment(opName, size);
      }
    }
  }

  @Override
  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append("File Stats").append("\n").append(fileStats.toString()).append("\n\n");
    sb.append("All Stats. Successfully cross checked").append("\n").append(allStats.toString()).append("\n\n");
    sb.append("All Stats. Cross check failed").append("\n").append(undeterminedOpsStats.toString()).append("\n");
    return sb.toString();
  }
}
