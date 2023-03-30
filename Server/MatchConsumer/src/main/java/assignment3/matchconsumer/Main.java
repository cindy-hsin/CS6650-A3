package assignment3.matchconsumer;

import static assignment3.config.constant.LoadTestConfig.CONSUMER_THREAD_NUM;

import assignment3.config.constant.LoadTestConfig;
import assignment3.config.constant.MongoConnectionInfo;
import assignment3.config.constant.RMQConnectionInfo;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.bson.Document;
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
    System.out.println("Connected to RMQ!");

    ConnectionString mongoUri = new ConnectionString(MongoConnectionInfo.uri);
    MongoClientSettings settings = MongoClientSettings.builder()
        .applyConnectionString(mongoUri)
        .applyToConnectionPoolSettings(builder ->
            builder
                .maxConnectionIdleTime(60, TimeUnit.SECONDS)
                .maxSize(LoadTestConfig.CONSUMER_DB_MAX_CONNECTION)
                .maxWaitTime(10, TimeUnit.SECONDS))
        .build();

    try {
      MongoClient mongoClient = MongoClients.create(settings);
      System.out.println("Connected to MongoDB!");
      // TODO: Do we need to lock the collection to avoid non-deterministic result on the documents?
      //  -> When multiple threads need to modify the same document...Or the operations
      //  chatgpt: update operations in MongoDB are atomic at the document level,
      //  meaning that a single update operation on a single document is guaranteed to be atomic.
      //  However, it's important to note that if your update operation involves multiple documents, ---> BULK!!
      //  the operation is not guaranteed to be atomic across all documents.
      //  In this case, you might need to use a transaction to ensure atomicity across multiple documents.
      //  .
      //  BUT ALSO: By default, MongoDB executes bulk write operations one-by-one in the specified order (i.e. serially)
      //  -> Seems to be ok?
      for (int i = 0; i < CONSUMER_THREAD_NUM; i++) {
        Runnable thread = new ConsumerThread(rmqConn, mongoClient);
        new Thread(thread).start();
      }

      System.out.println("Started all MatchConsumer Threads.");
    } catch (MongoException me) {
      System.out.println("Failed to create mongoClient: " + me);
    }



  }
}