package se.kth;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by salman on 5/2/16.
 */
public class LogLineParser {
  private static String getParameter(String str, String pattern, String paramName) {
    Pattern p = Pattern.compile(paramName + "=" + pattern);
    Matcher matcher = p.matcher(str);
    String value = null;
    if (matcher.find()) {
      value = matcher.group(0);
      value = value.substring((paramName + "=").length(), value.length());
    }
    return value;
  }

  public static HdfsExecutedOperation parseHdfsAuditLogLine(final String line) {
    String cmd = getParameter(line, "[a-zA-Z]+", "cmd");
    String src = getParameter(line, "\\S+", "src");
    if (src == "null") {
      src = null;
    }
    String dst = getParameter(line, "\\S+", "dst");
    if (dst == "null") {
      dst = null;
    }
    String allowedStr = getParameter(line, "[a-zA-Z]+", "allowed");
    boolean allowed = Boolean.parseBoolean(allowedStr);

    if (cmd != null && src != null && ValidHdfsOperations.isValidOperation(cmd) && allowed) {
      return new HdfsExecutedOperation(ValidHdfsOperations.HdfsOperationName.valueOf(cmd), src, dst, allowed);
    }
    return null;
  }
}
