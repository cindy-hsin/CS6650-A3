package thread;

import config.LoadTestConfig;
import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.MatchesApi;
import io.swagger.client.api.StatsApi;
import io.swagger.client.model.MatchStats;
import io.swagger.client.model.Matches;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import part2latency.Record;
import part2latency.RequestType;


public class GetThread extends AbsSendRequestThread {
  private static final int NUM_REQ_BATCH = 5;  // Required Constant! Not a tuning parameter
  private static final int GAP_TIME_MS = 1000; // Required Constant! Not a tuning parameter
  private static final int MIN_ID = 1;
  private static final int MAX_USER_ID = 50000;

  private final List<Record> records;


  public GetThread(CountDownLatch latch, AtomicInteger numSuccessfulReqs, AtomicInteger numFailedReqs, List<Record> getRecords) {
    super(numSuccessfulReqs, numFailedReqs, latch);
     this.records = getRecords;
  }

  @Override
  public void run() {
    MatchesApi matchesApi = new MatchesApi(new ApiClient());
    matchesApi.getApiClient().setBasePath(LoadTestConfig.GET_URL); //TODO: Change to Get Match servlet's url. Also change Post threads' url.

    StatsApi statsApi = new StatsApi(new ApiClient());
    statsApi.getApiClient().setBasePath(LoadTestConfig.GET_URL); //TODO: Change to Get Stats servlet's url
    System.out.println("Set base path");

    // Keep sending GET reqs until all PostThreads terminate. -> this.latch(which is the postLatch in Main)'s count == 0
    int apiType = 1;    // Mix up Matches and Stats requests


    while (this.latch.getCount() > 0) {
      Long batchStartTime = System.currentTimeMillis();
      System.out.println("GET batchStartTime: " + batchStartTime);
      for (int j = 0; j < NUM_REQ_BATCH; j++) {
        Record record = this.sendSingleRequest(matchesApi, statsApi, apiType);
        this.records.add(record);
        apiType *= -1;
      }
      Long batchEndTime = System.currentTimeMillis();

      try {
        Thread.sleep(GAP_TIME_MS - (batchStartTime - batchEndTime));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private Record sendSingleRequest(MatchesApi matchesApi, StatsApi statsApi, int apiType) {
    int retry = LoadTestConfig.MAX_RETRY;
    String userId = String.valueOf(ThreadLocalRandom.current().nextInt(MIN_ID, MAX_USER_ID+1)); //TODO: userId range: verify with server side(DB side)

    long startTime = System.currentTimeMillis();
    long endTime;

    int statusCode;
    while (retry > 0) {
      try {
        if (apiType == 1) {
          ApiResponse<Matches> res = matchesApi.matchesWithHttpInfo(userId);
          statusCode = res.getStatusCode();
          System.out.println("GET: MatchList for userId " + userId + ": " + res.getData().getMatchList());
        }
        else {
          ApiResponse<MatchStats> res = statsApi.matchStatsWithHttpInfo(userId);
          statusCode = res.getStatusCode();
          System.out.println("GET: Stats for userId " + userId + ": " + "likes -> " + res.getData().getNumLlikes() + " dislikes -> " + res.getData().getNumDislikes());
        }
        endTime = System.currentTimeMillis();
        numSuccessfulReqs.getAndIncrement();
        return new Record(startTime, RequestType.GET, (int)(endTime-startTime), statusCode, Thread.currentThread().getName());
      } catch (ApiException e) {
        String apiTypeStr = apiType == 1 ? "Match" : "Stats";

        if (e.getCode() == 400) {
          System.out.println("GET: Bad Request for userId " + userId + ": " + "Status Code: " + e.getCode() + " " + e.getResponseBody());
          endTime = System.currentTimeMillis();
          numSuccessfulReqs.getAndIncrement();
          return new Record(startTime, RequestType.GET, (int) (endTime - startTime), e.getCode(),
              Thread.currentThread().getName());
        } else if (e.getCode() == 404) {
          System.out.println("GET: " + apiTypeStr + " for userId " + userId + ": " + "Status Code: " + e.getCode()
              + " " + e.getResponseBody());
          endTime = System.currentTimeMillis();
          numSuccessfulReqs.getAndIncrement();
          return new Record(startTime, RequestType.GET, (int) (endTime - startTime), e.getCode(),
              Thread.currentThread().getName());
        }

        // If it's not "User Not Found" error or "Bad Request" error-> REAL FAIL!(network error) Failed to send the request. Need to RETRY.

        System.out.println(
            "GET: RETRY Request. Type: " + apiTypeStr + " userID:" + userId + "err: " + e.getCode()
                + " " + e.getResponseBody() + e.getMessage());
        retry--;
        if (retry == 0) {
          endTime = System.currentTimeMillis();
          numFailedReqs.getAndIncrement();
          System.out.println(
              "FAIL GET Request after all retries. Type: " + apiTypeStr + " userID:" + userId);
          return new Record(startTime, RequestType.GET, (int) (endTime - startTime), e.getCode(),
              Thread.currentThread().getName());
        }
      }
    }

    return null;
  }

}

