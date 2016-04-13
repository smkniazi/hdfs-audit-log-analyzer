package se.kth;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by salman on 4/12/16.
 * reads a single log file
 */
public class HdfsAuditLogReader {
  private final BufferedReader br;
  private boolean eofReached;

  public HdfsAuditLogReader(File filePath) throws FileNotFoundException {
    br = new BufferedReader(new FileReader(filePath));
    eofReached = false;
  }

  //parse lines in Hdfs Audit Log
  public HdfsExecutedOperation getHdfsOperation() throws IOException {
    if (eofReached) {
      throw new IOException("Parsed the whole fucking log. What else do you want from me.");
    }

    while(!eofReached) {
      String line = br.readLine();
      if (line == null) {
        eofReached = true;
        br.close();
        return null;
      }

      HdfsExecutedOperation operation = parseHdfsAuditLogLine(line);
      if(operation!=null){
        return operation;
      }
    }
    return null;
  }

  public boolean finishedReadingWholeFile(){
    return eofReached;
  }

  public String getParameter(String str, String pattern, String paramName){
    Pattern p = Pattern.compile(paramName+"="+pattern);
    Matcher matcher = p.matcher(str);
    String value = null;
    if (matcher.find()) {
      value = matcher.group(0);
      value = value.substring((paramName+"=").length(), value.length());
    }
    return value;
  }

  private HdfsExecutedOperation parseHdfsAuditLogLine(final String line) {
    String cmd = getParameter(line, "[a-zA-Z]+", "cmd");
    String src = getParameter(line, "\\S+", "src");
    String dst = getParameter(line, "\\S+", "dst");
    String allowedStr = getParameter(line, "[a-zA-Z]+", "allowed");
    boolean allowed = Boolean.parseBoolean(allowedStr);

    if (cmd != null && src != null && HdfsOperation.isValidOperation(cmd) && allowed) {
      return new HdfsExecutedOperation(HdfsOperation.HdfsOperationName.valueOf(cmd), src, dst, allowed);
    }
    return null;
  }
}
