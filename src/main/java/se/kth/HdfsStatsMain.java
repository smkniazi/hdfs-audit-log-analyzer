package se.kth;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by salman on 4/12/16.
 */
public class HdfsStatsMain {
  CmdLineParser parser;
  @Option(name = "-help", usage = "Print usages")
  private boolean help = false;
  @Option(name = "-Threads", usage = "Number of threads. Default is 10")
  private int threads = 10;
  @Option(name = "-webhdfs", usage = "Web HDFS address. Default is webhdfs://localhost:5978")
  private String webHdfsAddress = "webhdfs://localhost:5978";
  @Option(name = "-i", usage = "Dir/File path to HDFS audit log file(s). Default is ./logs")
  private String input = "./logs";
  @Option(name = "-o", usage = "Output File Path.")
  private String output = "./hdfs-stats.cvs";


  public static void main(String argv[]) throws Exception {
    (new HdfsStatsMain()).run(argv);
  }

  public void run(String argv[]) throws Exception {
    parseArgs(argv);
    HdfsStats hdfsStats = new HdfsStats();
    HdfsAuditLogsDirReader alr = new HdfsAuditLogsDirReader(input);
    ExecutorService es = Executors.newFixedThreadPool(threads);
    List workers = new ArrayList<WebHdfsCommunicator>(threads);
    for (int i = 0; i < threads; i++) {
      workers.add(new WebHdfsCommunicator(webHdfsAddress, alr, hdfsStats));
    }
    es.invokeAll(workers);
    String output = hdfsStats.toString();
    System.out.println(hdfsStats.toString());
    writeResults(output);
    System.exit(0);
  }

  private void parseArgs(String[] args) {
    parser = new CmdLineParser(this);
    parser.setUsageWidth(80);
    try {
      // parse the arguments.
      parser.parseArgument(args);
      if (help) {
        printHelp();
        System.exit(0);
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
      printHelp();
      System.exit(-1);
    }
  }

  private void printHelp() {
    parser.printUsage(System.out);
    System.out.println();
  }

  private void writeResults(String msg) throws FileNotFoundException {
    PrintWriter writer = new PrintWriter(output);
    writer.print(msg);
    writer.close();
  }
}