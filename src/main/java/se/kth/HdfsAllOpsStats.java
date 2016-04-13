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
    for (HdfsOperation.HdfsOperationName opName : stats.keySet()) {
      sb.append(opName).append("\n");
      sb.append(stats.get(opName));
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
      sb.append(DELIMETER).append(DELIMETER).append(DELIMETER).append("Total").append(DELIMETER).append
          (fileOpsCount+dirOpsCount)
          .append
          ("\n");
      sb.append(DELIMETER).append("FileOps").append(DELIMETER).append(fileOpsCount).append("\n");
      sb.append(DELIMETER).append("DirOps").append(DELIMETER).append(dirOpsCount).append("\n");
      return sb.toString();
    }
  }
}
