package assignment3.likesconsumer;


import assignment3.config.constant.RMQConnectionInfo;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
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

    // TODO: Create MongoClient (connection pool)


    // Integer[0]: num of likes, Integer[1]: num of dislikes
//    ConcurrentHashMap<String, int[]> map = new ConcurrentHashMap();
//
//    for (int i = 0; i < LIKES_CONSUMER_THREAD_NUM; i++) {
//      Runnable thread = new ConsumerThread(connection, map);
//      new Thread(thread).start();
//    }
//
//    System.out.println("Closed all LikesConsumer Threads.");
  }
}
