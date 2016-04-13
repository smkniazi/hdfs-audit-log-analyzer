package se.kth;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by salman on 2016-04-12.
 */
public class Progress {
  public static final long PROGRESS_DURATION = 5000;
  private static final AtomicLong successfullOps = new AtomicLong(0);
  private static final AtomicLong failedOps = new AtomicLong(0);
  private static final long startTime = System.currentTimeMillis();
  private static final AtomicLong lastPrintTime = new AtomicLong(0);

  public static void incrementSuccesfullOps() {
    successfullOps.incrementAndGet();
  }

  public static void incrementFailedOps() {
    failedOps.incrementAndGet();
  }

  public static synchronized void printPrgress() {
    try {
      long time = System.currentTimeMillis() - lastPrintTime.get();
      if (time > PROGRESS_DURATION) {
        lastPrintTime.set(System.currentTimeMillis());
        String msg = "Successfull Ops: " + successfullOps.get() + " Failed Stats: " + failedOps.get() + "  Speed: " +
            (successfullOps.get() + failedOps.get()) / ((System.currentTimeMillis() - startTime) / 1000) + " ops/sec";
//        System.out.write(msg.getBytes());
        System.out.println(msg);
      }
    } catch (Exception e) {

    }
  }
}
