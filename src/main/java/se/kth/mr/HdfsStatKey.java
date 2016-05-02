package se.kth.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import se.kth.mr.tools.ValidHdfsOperations;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by salman on 2016-05-02.
 */

public class HdfsStatKey implements WritableComparable<HdfsStatKey> {
  long sizeKey;
  ValidHdfsOperations.HdfsOperationName opName;
  ValidHdfsOperations.HdfsOperationType opType;

  public HdfsStatKey(){

  }

  public HdfsStatKey(long sizeKey, ValidHdfsOperations.HdfsOperationName opName,ValidHdfsOperations.HdfsOperationType
      opType) {
    this.sizeKey = getKey(sizeKey);
    this.opName = opName;
    this.opType = opType;
  }

  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVLong(out, sizeKey);
    WritableUtils.writeEnum(out, opName);
    WritableUtils.writeEnum(out, opType);
  }

  public void readFields(DataInput in) throws IOException {
    sizeKey = WritableUtils.readVLong(in);
    opName = WritableUtils.readEnum(in, ValidHdfsOperations.HdfsOperationName.class);
    opType = WritableUtils.readEnum(in, ValidHdfsOperations.HdfsOperationType.class);
  }

  public int compareTo(HdfsStatKey pop) {
    if (pop == null) {
      return 0;
    }
    int sizeComp = Long.compare(sizeKey, pop.sizeKey);
    if(sizeComp != 0){
      return sizeComp;
    } else {
      int opTypeComp = opType.toString().compareTo(pop.opType.toString());
      if(opTypeComp != 0){
        return opTypeComp;
      }else{
        return opName.toString().compareTo(pop.opName.toString());
      }
    }
  }

  private Long getKey(Long fileSize) {
    if(fileSize < 0) {
      return fileSize;
    }

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

  private String toHumanReadableFileSize(long sizeInBytes){
    long key = getKey(sizeInBytes);
    if(key < 0){
      return "NA";
    }

    if(key <= 1 * 1024 * 1024){
      return key/(1024)+"KB";
    }else{
      return key/(1024*1024)+"MB";
    }
  }

  @Override
  public String toString() {
    return sizeKey +"("+toHumanReadableFileSize(sizeKey)+")"+";"+opType+";"+opName;
  }
}
