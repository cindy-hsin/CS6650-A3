package assignment3.config.constant;

public class MongoConnectionInfo {
  public static final String userName = "";
  public static final String password = "";
  public static final String hostName = "127.0.0.1";

  public static final String port = "27017";

  //TODO: Add connection Options like maxPoolsize, either here in uri, or in Consumer's Main class
  // using builder pattern.
  public static final String uri = "mongodb://" + userName + ":" + password +
      "@" + hostName + ":" + port;

}
