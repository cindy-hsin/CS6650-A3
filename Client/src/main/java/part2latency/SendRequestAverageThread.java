package part2latency;

import config.LoadTestConfig;
import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.SwipeApi;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import request.PostRequest;
import request.PostRequestGenerator;
import thread.AbsSendRequestThread;


public class SendRequestAverageThread extends AbsSendRequestThread implements Runnable{
  private final BlockingQueue<List<Record>> recordsBuffer;


  public SendRequestAverageThread(CountDownLatch latch, AtomicInteger numSuccessfulReqs, AtomicInteger numFailedReqs,
      BlockingQueue<List<Record>> recordsBuffer) {
    super(numSuccessfulReqs, numFailedReqs, latch);
    this.recordsBuffer = recordsBuffer;
  }


  @Override
  public void run() {
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(LoadTestConfig.URL);

    SwipeApi swipeApi = new SwipeApi(apiClient);
    List<Record> records = new ArrayList<>();

    // Send multiple requests
    // Each thread averagely sends fixed amount of request.
    for (int i = 0; i < LoadTestConfig.NUM_TOTAL_REQUESTS / LoadTestConfig.NUM_THREADS; i++) {
      Record record = this.sendSingleRequestWithRecord(PostRequestGenerator.generateSingleRequest(), swipeApi, this.numSuccessfulReqs, this.numFailedReqs);
      records.add(record);
    }

    this.latch.countDown();

    try {
      recordsBuffer.put(records);
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed to put local list of records from " + Thread.currentThread().getName() + " to the buffer queue. " + e);
    }

  }


  private Record sendSingleRequestWithRecord(PostRequest request, SwipeApi swipeApi, AtomicInteger numSuccessfulReqs, AtomicInteger numFailedReqs) {
    int retry = LoadTestConfig.MAX_RETRY;

    long startTime = System.currentTimeMillis();
    long endTime;
    while (retry > 0) {
      try {
        ApiResponse res = swipeApi.swipeWithHttpInfo(request.getBody(), request.getSwipeDir());

        endTime = System.currentTimeMillis();
        numSuccessfulReqs.getAndIncrement();
        System.out.println("Thread:" + Thread.currentThread().getName() + " Success cnt:" + numSuccessfulReqs.get() + " Status:" + res.getStatusCode());
        return new Record(startTime, RequestType.POST, (int)(endTime-startTime), res.getStatusCode(), Thread.currentThread().getName());
      } catch (ApiException e) {
        System.out.println("Failed to send request: " + e.getCode() + ": " + e.getResponseBody() + ".request.Request details:"
            + request.getSwipeDir() + " " + request.getBody().toString() + ". Go retry");
        retry --;
        if (retry == 0) {
          endTime = System.currentTimeMillis();
          numFailedReqs.getAndIncrement();
          return new Record(startTime, RequestType.POST, (int)(endTime-startTime), e.getCode(), Thread.currentThread().getName());
        }
      }
    }

    return null;
  }

}
