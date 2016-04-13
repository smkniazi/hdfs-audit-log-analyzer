package se.kth;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by salman on 2016-04-12.
 */
public class HdfsAllOpsStats {
  private final String DELIMETER = ";\t";
  private Map<HdfsOperation.HdfsOperationName, Stat> stats = new HashMap<HdfsOperation.HdfsOperationName, Stat>();

  public void increment(HdfsOperation.HdfsOperationName opName, boolean isDirOp) {
    Stat stat = getStatObj(opName);
    stat.increment(isDirOp);
  }

  private Stat getStatObj(HdfsOperation.HdfsOperationName opName) {
    Stat stat = stats.get(opName);
    if (stat == null) {
      stat = new Stat();
      stats.put(opName, stat);
    }
    return stat;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("OpName").append("FileOperations").append(DELIMETER).append("DirOps").append(DELIMETER).append("Total")
        .append("\n");
    for (HdfsOperation.HdfsOperationName opName : HdfsOperation.getValidOpsSet()) {
      Stat stat = stats.get(opName);
      if(stat != null){
        sb.append(opName).append(DELIMETER).append(stats.get(opName)).append("\n");
      }else{
        sb.append(opName).append(DELIMETER).append(DELIMETER).append(DELIMETER).append("\n");
      }
    }
    return sb.toString();
  }

  private class Stat {
    private int fileOpsCount;
    private int dirOpsCount;

    public int getFileOpsCount() {
      return fileOpsCount;
    }

    public int getDirOpsCount() {
      return dirOpsCount;
    }

    public void increment(boolean isDirOp) {
      if (isDirOp) {
        dirOpsCount++;
      } else {
        fileOpsCount++;
      }
    }

    @Override
    public String toString(){
      StringBuilder sb = new StringBuilder();
      sb.append(fileOpsCount).append(DELIMETER).append(dirOpsCount).append(DELIMETER).append(fileOpsCount+dirOpsCount);
      return sb.toString();
    }
  }
}
