package part2latency;


import config.LoadTestConfig;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import thread.GetThread;


public class MainPart2 {
  private static final int QUEUE_CAPACITY = LoadTestConfig.NUM_THREADS;
  //private static final String ALL_RECORDS_CSV = "AllRecords.csv";
  //private static final String START_TIME_GROUP_CSV = "StartTimeGroupedRequests.csv";
  private static final RunningMetrics postMetrics = new RunningMetrics();
  private static final RunningMetrics getMetrics = new RunningMetrics();

  private static Long startPostTime, endPostTime;

  public static void main(String[] args) throws InterruptedException, CsvExistException {
    CountDownLatch postLatch = new CountDownLatch(LoadTestConfig.NUM_THREADS);
    final AtomicInteger numSuccessfulPostReqs = new AtomicInteger(0);
    final AtomicInteger numFailedPostReqs = new AtomicInteger(0);
    final AtomicInteger numSuccessfulGetReqs = new AtomicInteger(0);
    final AtomicInteger numFailedGetReqs = new AtomicInteger(0);

    BlockingQueue<List<Record>> postRecordsBuffer = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    List<Record> getRecords = new ArrayList<>();


    startPostTime = System.currentTimeMillis();
    endPostTime = null;

//    if (new File(ALL_RECORDS_CSV).isFile()) {
//      throw new CsvExistException(ALL_RECORDS_CSV);
//    }

    // Start POST threads
    for (int i = 0; i < LoadTestConfig.NUM_THREADS; i++) {
      Runnable thread = new SendRequestAverageThread(postLatch, numSuccessfulPostReqs, numFailedPostReqs, postRecordsBuffer);
      new Thread(thread).start();
    }

    // Start the Get thread
    new Thread(new GetThread(postLatch, numSuccessfulGetReqs, numFailedGetReqs, getRecords)).start();


    int numRecordListsTaken = 0;
//    List<Record> inMemoryAllRecords = new ArrayList<>();
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

    // TODO: Confirm: GetThread will terminate automatically, once all PostThreads terminate.

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

//  private static void updateRunningMetrics(List<Record> records) {
//    for (Record record: records) {
//      if (record.getResponseCode() == LoadTestConfig.SUCCESS_CODE) {
//        int curLatency = record.getLatency();
//        postMetrics.setMinLatency(Math.min(curLatency, postMetrics.getMinLatency()));
//        postMetrics.setMaxLatency(Math.max(curLatency, postMetrics.getMaxLatency()));
//        postMetrics.setSumLatency(postMetrics.getSumLatency() + curLatency);
//        postMetrics.setNumTotalRecord(postMetrics.getNumTotalRecord() + 1);
//        // postMetrics.incrementStartTimeGroupCount(startTime, record.getStartTime());
//      }
//    }
//  }

/**
  private static void writeAllRecords(List<Record> records) {
    try (BufferedWriter outputFile = new BufferedWriter(new FileWriter(ALL_RECORDS_CSV, true))) {
      String line;

      for (Record record: records) {
        line = record.toString();
        outputFile.write(line + System.lineSeparator());
      }
    } catch (FileNotFoundException fnfe) {
      System.out.println("*** Write: CSV file was not found : " + fnfe.getMessage());
      fnfe.printStackTrace();
    } catch (IOException ioe) {
      System.out.println("Error when writing to CSV " + ALL_RECORDS_CSV + ": " + ioe.getMessage());
      ioe.printStackTrace();
    }
  }

  private static void readCsvToGroupLatency() {
    try (BufferedReader inputFile = new BufferedReader(new FileReader(ALL_RECORDS_CSV))) {

      String line;
      while ((line = inputFile.readLine()) != null) {
        String[] record = line.split(",");
        if (Integer.valueOf(record[3]).equals(LoadTestConfig.SUCCESS_CODE)) {
          int latency = Integer.valueOf(record[2]);
          postMetrics.incrementLatencyGroupCount(latency);
        }
      }
    } catch (FileNotFoundException fnfe) {
      System.out.println("*** Read: CSV file was not found : " + fnfe.getMessage());
      fnfe.printStackTrace();
    } catch (IOException ioe) {
      System.out.println("Error when reading CSV " + ALL_RECORDS_CSV + ": " + ioe.getMessage());
      ioe.printStackTrace();
    }
  }


  private static void writeToCsvByStartTime(Map<Integer, Integer> startTimeGroupCount) {
    Map<Integer, Integer> sortedMap = new TreeMap<>(startTimeGroupCount);  // sort by key(second from programStartTime)

    try (BufferedWriter outputFile = new BufferedWriter(new FileWriter(START_TIME_GROUP_CSV))) {
      String line;

      for (Integer second: sortedMap.keySet()) {
        line = second + "," + sortedMap.get(second);
        outputFile.write(line + System.lineSeparator());
      }
    } catch (FileNotFoundException fnfe) {
      System.out.println("*** CSV file was not found : " + fnfe.getMessage());
      fnfe.printStackTrace();
    } catch (IOException ioe) {
      System.out.println("Error when writing to CSV : " + ioe.getMessage());
      ioe.printStackTrace();
    }
  }
**/
  /**
   * Ues InMemory Collection & DescriptiveStatistics class
   * to calculate the correct statistics (Only for TESTING PURPOSE)
   */
  /**
  private static void testCalcStatistics( List<Record> inMemoryAllRecords) {

    DescriptiveStatistics ds = new DescriptiveStatistics();

    for (Record record: inMemoryAllRecords) {
      ds.addValue(record.getLatency());
    }
    System.out.println("====== TEST: Statistics calculated with in memory collection and DescriptiveStatistics ========");
    System.out.println("Mean Response Time (ms): " + ds.getMean());
    System.out.println("Median Response Time (ms): "+ds.getPercentile(50));
    System.out.print("99th Percentile Response Time: " + ds.getPercentile(99));
    System.out.println("Min Response Time (ms): "+ds.getMin());
    System.out.println("Max Response Time (ms): " + ds.getMax());
  }
*/
}
