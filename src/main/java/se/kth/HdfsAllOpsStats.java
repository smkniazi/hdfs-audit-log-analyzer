package se.kth;

import se.kth.mr.tools.ValidHdfsOperations;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by salman on 2016-04-12.
 */
public class HdfsAllOpsStats implements Serializable{
  private final String DELIMETER = ";\t";
  private Map<ValidHdfsOperations.HdfsOperationName, Stat> stats = new HashMap<ValidHdfsOperations.HdfsOperationName, Stat>();

  public void increment(ValidHdfsOperations.HdfsOperationName opName, boolean isDirOp) {
    Stat stat = getStatObj(opName);
    stat.increment(isDirOp);
  }

  private Stat getStatObj(ValidHdfsOperations.HdfsOperationName opName) {
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
    sb.append("OpName").append(DELIMETER).append("FileOperations").append(DELIMETER).append("DirOps").append
        (DELIMETER).append
        ("Total")
        .append("\n");
    for (ValidHdfsOperations.HdfsOperationName opName : ValidHdfsOperations.getValidOpsSet()) {
      Stat stat = stats.get(opName);
      if(stat != null){
        sb.append(opName).append(DELIMETER).append(stats.get(opName)).append("\n");
      }else{
        sb.append(opName).append(DELIMETER).append("0").append(DELIMETER).append("0").append(DELIMETER).append("0")
            .append("\n");
      }
    }
    return sb.toString();
  }

  private class Stat implements Serializable{
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
