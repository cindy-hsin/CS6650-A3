package assignment3.config.constant;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
public class RMQConnectionInfo {

  private final static String RMQ_IP = "35.89.19.68"; //Large: 34.217.78.115 Small: 34.219.5.26
  public final static Map<String, String> RMQ_SERVER_CONFIG  = Stream.of(new String[][] {
      { "userName", "cindychen" },    // localhost: guest;
      { "password", "password" },     // localhost: guest; password
      { "virtualHost", "swipe_broker" },  // localhost: "/"; swipe_broker
      { "hostName", RMQ_IP },
      { "portNumber", "5672" }      // 5672 is for RabbitMQ server. 15672 is to access management console. https://stackoverflow.com/a/69523757
  }).collect(Collectors.toMap(data -> data[0], data -> data[1]));

  public final static String EXCHANGE_NAME = "swipedata";


}
