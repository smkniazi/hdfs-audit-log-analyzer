package se.kth.mr.tools;

/**
 * Created by salman on 4/12/16.
 */
public class HdfsExecutedOperation {

  private final ValidHdfsOperations.HdfsOperationName opName;
  private final String src;
  private final String dst;
  private final boolean allowed;

  public HdfsExecutedOperation(ValidHdfsOperations.HdfsOperationName opName, String path, String dst, boolean allowed) {
    this.opName = opName;
    this.src = path;
    this.dst = dst;
    this.allowed = allowed;
  }



  public ValidHdfsOperations.HdfsOperationName getOpName() {
    return opName;
  }

  public String getSrc() {
    return src;
  }

  public String getDst() {
    return dst;
  }

  public boolean isAllowed() {
    return allowed;
  }

  @Override
  public String toString() {
    return "Command: " + opName + " allowed: "+allowed+" src: " + src+" dst: "+dst;
  }


}