package assignment3.matchconsumer;

import static assignment3.config.constant.LoadTestConfig.MATCH_CONSUMER_THREAD_NUM;

import assignment3.config.constant.MongoConnectionInfo;
import assignment3.config.constant.RMQConnectionInfo;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

public class Main {

  public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
    ConnectionFactory rmqConnFactory = new ConnectionFactory();
    // Connect to RMQ server. Ref: https://www.rabbitmq.com/api-guide.html#connecting
    rmqConnFactory.setUsername(RMQConnectionInfo.RMQ_SERVER_CONFIG.get("userName"));
    rmqConnFactory.setPassword(RMQConnectionInfo.RMQ_SERVER_CONFIG.get("password"));
    rmqConnFactory.setVirtualHost(RMQConnectionInfo.RMQ_SERVER_CONFIG.get("virtualHost"));
    rmqConnFactory.setHost(RMQConnectionInfo.RMQ_SERVER_CONFIG.get("hostName"));
    rmqConnFactory.setPort(Integer.valueOf(RMQConnectionInfo.RMQ_SERVER_CONFIG.get("portNumber")));

    Connection rmqConn = rmqConnFactory.newConnection();

    ConnectionString mongoUri = new ConnectionString(MongoConnectionInfo.uri);
    MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(mongoUri).build();

    MongoClient mongoClient = MongoClients.create();

    for (int i = 0; i < MATCH_CONSUMER_THREAD_NUM; i++) {
      Runnable thread = new ConsumerThread(rmqConn, mongoClient);
      new Thread(thread).start();
    }

    System.out.println("Closed all MatchConsumer Threads.");

  }
}