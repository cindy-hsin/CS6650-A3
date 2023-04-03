package part2latency;


import config.LoadTestConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import thread.GetThread;
import thread.PostThread;


public class MainPart2 {
  private static final int QUEUE_CAPACITY = LoadTestConfig.NUM_THREADS;

  private static final RunningMetrics postMetrics = new RunningMetrics();
  private static final RunningMetrics getMetrics = new RunningMetrics();

  private static Long startPostTime, endPostTime;

  public static void main(String[] args) throws InterruptedException {
    CountDownLatch postLatch = new CountDownLatch(LoadTestConfig.NUM_THREADS);
    final AtomicInteger numSuccessfulPostReqs = new AtomicInteger(0);
    final AtomicInteger numFailedPostReqs = new AtomicInteger(0);
    final AtomicInteger numSuccessfulGetReqs = new AtomicInteger(0);
    final AtomicInteger numFailedGetReqs = new AtomicInteger(0);

    BlockingQueue<List<Record>> postRecordsBuffer = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    List<Record> getRecords = new ArrayList<>();


    startPostTime = System.currentTimeMillis();
    endPostTime = null;


    // Start POST threads
    final AtomicInteger numTakenReqs = new AtomicInteger(0);
    for (int i = 0; i < LoadTestConfig.NUM_THREADS; i++) {
      Runnable thread = new PostThread(postLatch, numSuccessfulPostReqs, numFailedPostReqs, numTakenReqs, postRecordsBuffer);
      new Thread(thread).start();
    }

    System.out.println(" ========= Start! GET thread =========");
    // Start the GET thread
    new Thread(new GetThread(postLatch, numSuccessfulGetReqs, numFailedGetReqs, getRecords)).start();

    // Update Metrics for POST records
    int numRecordListsTaken = 0;
    while (numRecordListsTaken < LoadTestConfig.NUM_THREADS || endPostTime == null) {
      if (postRecordsBuffer.size() > 0) {
        // take from buffer
        List<Record> threadRecords = postRecordsBuffer.take(); // Might throw InterruptedException
        numRecordListsTaken ++;
        // Iterate through each record: Update, max, min, sum; increment count to time group (starting at which second)
        postMetrics.updateRunningMetrics(threadRecords);
        // writeAllRecords(threadRecords);
        // inMemoryAllRecords.addAll(threadRecords); // FOR TESTING PURPOSE
      } else if  (postLatch.getCount() == 0) {
        // Mark endTime
        endPostTime = System.currentTimeMillis();
      }
    }

    // Update Metrics for GET records
    getMetrics.updateRunningMetrics(getRecords);

    // endTime might contain one unnecessary FileWrite time.
    // CASE1:
    // When the Last thread put a list to queue and call latch.countDown(),
    // if main thread happens to be in the latch block, then endTime will be accurately marked,
    // and then main thread goes on to take the last list from queue. END while loop.

    // CASE2:
    // When the Last thread put a list to queue and call latch.countDown().
    // if main thread happens to be in the postRecordsBuffer block, then main thread will
    // take the last list, WRITE TO CSV, and then mark endTime.
    // In this case, the endTime will be longer than the actual request-sending endTime, by a difference of
    // WRITE_ONE_LIST_TO_CSV TIME.

    getMetrics.updateRunningMetrics(getRecords);

    float wallTime = (endPostTime - startPostTime)/1000f;
    System.out.println("====== POST requests results ======");
    System.out.println("Successful Requests:" + numSuccessfulPostReqs);
    System.out.println("Unsuccessful Requests:" + numFailedPostReqs);
    System.out.println("Number of Threads: " + LoadTestConfig.NUM_THREADS);
    System.out.println("Multi-thread wall time:" + wallTime + "s");
    System.out.println("Throughput: " + numSuccessfulPostReqs.get() / wallTime + " req/s");
    System.out.println("\n");

    System.out.println("Mean Response Time (ms): " + (float)postMetrics.getSumLatency() / postMetrics.getNumTotalRecord());
    // System.out.println("Median Response Time (ms): " + postMetrics.calPercentileLatency(50));
    System.out.println("Throughput (req/s): " + (float)(numSuccessfulPostReqs.get()) / (endPostTime - startPostTime) * 1000);
    // System.out.println("99th Percentile Response Time: " + postMetrics.calPercentileLatency(99));
    System.out.println("Min Response Time (ms): " + postMetrics.getMinLatency());
    System.out.println("Max Response Time (ms): " + postMetrics.getMaxLatency());

    System.out.println("\n\n====== GET requests results ======");
    System.out.println("Successful Requests:" + numSuccessfulGetReqs);
    System.out.println("Unsuccessful Requests:" + numFailedGetReqs);
    System.out.println("Mean Response Time (ms): " + (float)getMetrics.getSumLatency() / getMetrics.getNumTotalRecord());
    System.out.println("Min Response Time (ms): " + getMetrics.getMinLatency());
    System.out.println("Max Response Time (ms): " + getMetrics.getMaxLatency());

    // TEST:
    // testCalcStatistics(inMemoryAllRecords);
  }



}
