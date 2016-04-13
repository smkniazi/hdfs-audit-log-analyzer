package se.kth;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by salman on 2016-04-13.
 */
public class HdfsOperationDepthStat {
  private long count;
  private long sum;

  public HdfsOperationDepthStat() {
    this.count = 0;
    this.sum = 0;
  }

  public synchronized void addOperationDepth(int depth){
    sum+=depth;
    count++;
  }

  public synchronized double getAverageOperationDepth(){
    if(count > 0 ) {
      return (double) sum / (double) count;
    }else{
      return 0;
    }
  }
}
