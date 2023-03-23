package thread;

import config.LoadTestConfig;
import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.MatchesApi;
import io.swagger.client.model.Matches;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import part2latency.Record;
import part2latency.RequestType;


public class GetThread extends AbsSendRequestThread {
  private static final int NUM_REQ_BATCH = 5;
  private static final int GAP_TIME_MS = 1000;
  private static final int MIN_ID = 1;
  private static final int MAX_USER_ID = 1000000;

  private final List<Record> records;


  public GetThread(CountDownLatch latch, AtomicInteger numSuccessfulReqs, AtomicInteger numFailedReqs, List<Record> getRecords) {
    super(numSuccessfulReqs, numFailedReqs, latch);
     this.records = getRecords;
  }

  @Override
  public void run() {
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(LoadTestConfig.URL);//TODO: Change to Get servlet's url. Also change Post threads' url.

    MatchesApi matchesApi = new MatchesApi(apiClient);
    // Keep sending GET reqs until all PostThreads terminate. -> this.latch(which is the postLatch in Main)'s count == 0
    while (this.latch.getCount() == 0) {
      for (int j = 0; j < NUM_REQ_BATCH; j++) {
        Record record = this.sendSingleRequest(matchesApi);
        this.records.add(record);
      }
    }
  }

  private Record sendSingleRequest(MatchesApi matchesApi) { // TODO: num of successful reqs?
    int retry = LoadTestConfig.MAX_RETRY;
    String userId = String.valueOf(ThreadLocalRandom.current().nextInt(MIN_ID, MAX_USER_ID+1)); //TODO: verify with server side(DB side)

    long startTime = System.currentTimeMillis();
    long endTime;

    while (retry > 0) {
      try {
        ApiResponse<Matches> res = matchesApi.matchesWithHttpInfo(userId);

        endTime = System.currentTimeMillis();
        System.out.println("MatchList for userId " + userId + ": " + res.getData().getMatchList()); //getMatchList());
        return new Record(startTime, RequestType.GET, (int)(endTime-startTime), res.getStatusCode(), Thread.currentThread().getName());
      } catch (ApiException e) {
        System.out.println("Retry GET request");
        retry --;
        if (retry == 0) {
          endTime = System.currentTimeMillis();
          numFailedReqs.getAndIncrement();
          return new Record(startTime, RequestType.GET, (int)(endTime-startTime), e.getCode(), Thread.currentThread().getName());
        }
      }
    }

    return null;
  }
}
