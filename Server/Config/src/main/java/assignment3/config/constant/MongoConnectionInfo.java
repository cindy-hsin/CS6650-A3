package assignment3.config.constant;

public class MongoConnectionInfo {
  public static final String USER_NAME = "";
  public static final String PASSWORD = "";
  public static final String HOST_NAME = "127.0.0.1";

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
}
