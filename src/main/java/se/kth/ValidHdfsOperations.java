package se.kth;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by salman on 2016-04-13.
 */
public class ValidHdfsOperations {

  private static final Map<String, String> VALID_OPERATION = new HashMap<String, String>();

  static {
    HdfsOperationName[] possibleValues = HdfsOperationName.class.getEnumConstants();
    for (HdfsOperationName op : possibleValues) {
      VALID_OPERATION.put(op.toString(), op.toString());
    }
  }

  public static boolean isValidOperation(String operation) {
    return VALID_OPERATION.containsKey(operation);
  }

  public static Set<HdfsOperationName> getValidOpsSet() {
    Set<HdfsOperationName> sortedNames = new TreeSet();
    for (HdfsOperationName opName : HdfsOperationName.class.getEnumConstants()) {
      sortedNames.add(opName);
    }
    return sortedNames;
  }

  public enum HdfsOperationType {
    DirOp("DirOp"),
    FileOp("FileOp"),
    UnDetermined("UnDetermined");

    private final String name;

    private HdfsOperationType(String name) {
      this.name = name;
    }

    public boolean equalsName(String otherName) {
      return (otherName == null) ? false : name.equals(otherName);
    }

    public String toString() {
      return name;
    }
  }

  public enum HdfsOperationName {
    open("open"),
    create("create"),
    delete("delete"),
    rename("rename"),
    mkdirs("mkdirs"),
    listStatus("listStatus"),
    setReplication("setReplication"),
    setOwner("setOwner"),
    setPermission("setPermission"),
    setTimes("setTimes"),
    setXAttr("setXAttr"),
    truncate("truncate"),
    concat("concat"),
    contentSummary("contentSummary"),
    setQuota("setQuota"),
    append("append"),
    getfileinfo("getfileinfo"),
    isFileClosed("isFileClosed"),
    listXAttrs("listXAttrs"),
    createSymlink("createSymlink");

    private final String name;

    private HdfsOperationName(String name) {
      this.name = name;
    }

    public boolean equalsName(String otherName) {
      return (otherName == null) ? false : name.equals(otherName);
    }

    public String toString() {
      return this.name;
    }
  }
}
