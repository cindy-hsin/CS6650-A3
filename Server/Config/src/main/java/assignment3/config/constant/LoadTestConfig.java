package assignment3.config.constant;

public class LoadTestConfig {
  public final static int POST_SERVLET_CHANNEL_POOL_SIZE = 50;
  public final static int CONSUMER_THREAD_NUM = 50;   // num of connections from Consumer to DB
  // The actual DB connection from Consumer = min(CONSUMER_THREAD_NUM, CONSUMER_DB_MAX_CONNECTION)
  public final static int CONSUMER_DB_MAX_CONNECTION = 200;
  public static final int MATCHES_SERVLET_DB_MAX_CONNECTION = 50;
  public static final int STATS_SERVLET_DB_MAX_CONNECTION = 50;

  public static final int BATCH_UPDATE_SIZE = 5;   // Consumer.

  // CONSUMER_THREAD_NUM * BATCH_UPDATE_SIZE must be divisble by 500K,
  // so that all msgs can be batch updated and ACK to RMQ??
}
