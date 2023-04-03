package assignment3.config.constant;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import java.util.concurrent.TimeUnit;

public class MongoConnectionInfo {
  public static final String USER_NAME = "";
  public static final String PASSWORD = "";
  public static final String HOST_NAME = "34.220.225.59"; // local: "127.0.0.1";

  public static final String PORT = "27017";

  public static final String DATABASE = "twinder";

  public static final String MATCH_COLLECTION = "Matches";
  public static final String STATS_COLLECTION = "Stats";


  //TODO: Add connection Options like maxPoolsize, either here in uri, or in Consumer's Main class
  // using builder pattern.
//  public static final String uri = "mongodb://" + USER_NAME + ":" + PASSWORD +
//      "@" + HOST_NAME + ":" + PORT;
  public static final String uri = "mongodb://" +
       HOST_NAME + ":" + PORT;


  public static MongoClientSettings buildMongoSettings(String servletClassName) {
    MongoClientSettings settings = MongoClientSettings.builder()
        .applyConnectionString(new ConnectionString(MongoConnectionInfo.uri))
        .applyToConnectionPoolSettings(builder ->
            builder
                .maxConnectionIdleTime(60, TimeUnit.SECONDS)
                .maxSize(servletClassName == "Matches" ?
                    LoadTestConfig.MATCHES_SERVLET_DB_MAX_CONNECTION :
                    LoadTestConfig.STATS_SERVLET_DB_MAX_CONNECTION)
                .maxWaitTime(10, TimeUnit.SECONDS))
        .build();

    return settings;
  }

}
