package se.kth;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by salman on 2016-04-12.
 */
public class HdfsFileOpsStats {

  private final String DELIMETER = ";\t";
  private Map<Long, Stat> stats = new HashMap<Long, Stat>();

  public void increment(HdfsOperation.HdfsOperationName opName, Long fileSize) {
    long key = getKey(fileSize);
    Stat stat = getStatObj(key);
    stat.increment(opName);
  }

  private Stat getStatObj(Long key) {
    Stat stat = stats.get(key);
    if (stat == null) {
      stat = new Stat();
      stats.put(key, stat);
    }
    return stat;
  }

  private Long getKey(Long fileSize) {
    //if the file is less than a MB then the key granularity will be In KB
    //other wise the key granularity will be in MB
    long keyGranularity;
    long key;
    if (fileSize <= 1 * 1024 * 1024) {
      keyGranularity = 1024;
    } else {
      keyGranularity = 8 * 1024 * 1024;
    }

    long quotient = fileSize/keyGranularity;
    long remainder = fileSize%keyGranularity;

    key = quotient*keyGranularity;
    if(remainder != 0){
      key += keyGranularity;
    }

    return new Long(key);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append("FileSize").append(DELIMETER);
    for (HdfsOperation.HdfsOperationName op : HdfsOperation.getValidOpsSet()) {
      sb.append(op).append(DELIMETER);
    }
    sb.append("\n");

    Set<Long> sortedKeys = new TreeSet<Long>();
    sortedKeys.addAll(stats.keySet());

    for (Long fileSize : sortedKeys) {
      Stat stat = stats.get(fileSize);
      sb.append(fileSize).append(DELIMETER).append(stat).append("\n");
    }
    return sb.toString();
  }

  public class Stat {
    private Map<HdfsOperation.HdfsOperationName, Long> stats = new HashMap<HdfsOperation.HdfsOperationName, Long>();

    public void increment(HdfsOperation.HdfsOperationName opName) {
      Long stat = stats.get(opName);
      if (stat == null) {
        stat = new Long(1);
        stats.put(opName, stat);
      } else {
        stat = new Long(stat + 1);
        stats.put(opName, stat);
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (HdfsOperation.HdfsOperationName key : HdfsOperation.getValidOpsSet()) {
        Long count = stats.get(key);
        if (count == null) {
          count = new Long(0);
        }
        sb.append(count).append(DELIMETER);
      }
      return sb.toString();
    }
  }
}