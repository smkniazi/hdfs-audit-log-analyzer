package se.kth;

import se.kth.mr.tools.HdfsExecutedOperation;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by salman on 4/12/16.
 * reads all the audit log files in a directory
 */
public class HdfsAuditLogsDirReader {
  private final List<File> files;
  private HdfsAuditLogReader currentLogReader = null;

  public HdfsAuditLogsDirReader(String dirPath) throws Exception {
    files = findFiles(dirPath);
    if (files.size() == 0) {
      throw new Exception("The input directory is empty");
    }
  }

  private List<File> findFiles(String path) {
    List<File> allResultFiles = new ArrayList<File>();
    File root = new File(path);
    if (!root.isDirectory()) {
      System.err.println(path + " is not a directory. Specify a directory that contains all the audit files");
      return allResultFiles;
    }

    List<File> dirs = new ArrayList<File>();
    dirs.add(root);
    while (!dirs.isEmpty()) {
      File dir = dirs.remove(0);

      File[] contents = dir.listFiles();
      if (contents != null && contents.length > 0) {
        for (File content : contents) {
          if (content.isDirectory()) {
            dirs.add(content);

          } else {
            System.out.println("Found a file  " + content.getAbsolutePath());
            allResultFiles.add(content);
          }
        }
      }
    }
    return allResultFiles;
  }


  public synchronized HdfsExecutedOperation getHdfsOperation() throws IOException {
    HdfsAuditLogReader reader;
    do {
      reader = getNextHdfsLogReader();
      if (reader != null) {
        HdfsExecutedOperation operation = reader.getHdfsOperation();
        if (operation != null) {
          return operation;
        }
      }
    } while (reader != null);
    return null;
  }

  private HdfsAuditLogReader getNextHdfsLogReader() throws FileNotFoundException {
    if (currentLogReader == null || currentLogReader.finishedReadingWholeFile()) {
      if (files.size() > 0) {
        File logFile = files.remove(0);
        System.out.println("Reading File " + logFile.getAbsolutePath());
        currentLogReader = new HdfsAuditLogReader(logFile);
      } else {
        currentLogReader = null;
      }
    }
    return currentLogReader;
  }
}

