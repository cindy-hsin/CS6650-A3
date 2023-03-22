package assignment3.matchconsumer;

import static assignment3.config.constant.LoadTestConfig.MATCH_CONSUMER_THREAD_NUM;

import assignment3.config.constant.RMQConnectionInfo;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

public class Main {

  public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
    ConnectionFactory connectionFactory = new ConnectionFactory();
    // Connect to RMQ server. Ref: https://www.rabbitmq.com/api-guide.html#connecting
    connectionFactory.setUsername(RMQConnectionInfo.RMQ_SERVER_CONFIG.get("userName"));
    connectionFactory.setPassword(RMQConnectionInfo.RMQ_SERVER_CONFIG.get("password"));
    connectionFactory.setVirtualHost(RMQConnectionInfo.RMQ_SERVER_CONFIG.get("virtualHost"));
    connectionFactory.setHost(RMQConnectionInfo.RMQ_SERVER_CONFIG.get("hostName"));
    connectionFactory.setPort(Integer.valueOf(RMQConnectionInfo.RMQ_SERVER_CONFIG.get("portNumber")));

    Connection connection = connectionFactory.newConnection();

    ConcurrentHashMap<String, Set<String>> map = new ConcurrentHashMap();

    for (int i = 0; i < MATCH_CONSUMER_THREAD_NUM; i++) {
      Runnable thread = new ConsumerThread(connection, map);
      new Thread(thread).start();
    }

    System.out.println("Closed all MatchConsumer Threads.");




  }
}