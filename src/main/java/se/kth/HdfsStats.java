package se.kth;

import se.kth.mr.tools.ValidHdfsOperations;

import java.io.Serializable;

/**
 * Created by salman on 2016-04-12.
 */
public class HdfsStats implements Serializable{
  private final HdfsAllOpsStats allStats;
  private final HdfsFileOpsStats fileStats;
  private final HdfsAllOpsStats undeterminedOpsStats;
  private final HdfsOperationDepthStat hdfsOperationDepthStat;
  public HdfsStats(){
    this.allStats = new HdfsAllOpsStats();
    this.fileStats = new HdfsFileOpsStats();
    this.undeterminedOpsStats = new HdfsAllOpsStats();
    this.hdfsOperationDepthStat = new HdfsOperationDepthStat();
  }

  public synchronized  void increment(ValidHdfsOperations.HdfsOperationName opName, ValidHdfsOperations.HdfsOperationType opType,
                                      Long size, int depth){
    hdfsOperationDepthStat.addOperationDepth(depth);
    if(opType == ValidHdfsOperations.HdfsOperationType.UnDetermined){
      undeterminedOpsStats.increment(opName, false);
    }else {
      boolean isDirOp = opType == ValidHdfsOperations.HdfsOperationType.DirOp;
      allStats.increment(opName, isDirOp);
      if (opType == ValidHdfsOperations.HdfsOperationType.FileOp) {
        fileStats.increment(opName, size,depth);
      }
    }
  }

  @Override
  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append("Average Operational Depth is: "+hdfsOperationDepthStat.getAverageOperationDepth()).append("\n");
    sb.append("File Stats").append("\n").append(fileStats.toString()).append("\n\n");
    sb.append("All Stats. Successfully cross checked").append("\n").append(allStats.toString()).append("\n\n");
    sb.append("All Stats. Cross check failed").append("\n").append(undeterminedOpsStats.toString()).append("\n");
    return sb.toString();
  }
}
