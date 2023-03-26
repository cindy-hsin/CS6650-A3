package assignment3.matchconsumer;

import assignment3.config.constant.RMQConnectionInfo;
import assignment3.config.datamodel.SwipeDetails;
import com.google.gson.Gson;
import com.mongodb.client.MongoClient;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

// MatchConsumerThread
public class ConsumerThread implements Runnable{

  private static final String QUEUE_NAME = "match";
  private static final int MAX_MATCH_SIZE = 100;
  private static final int BATCH_UPDATE_SIZE = 100;
  private static final Gson gson = new Gson();
  private Connection connection;
  private MongoClient mongoClient;

  private Map<String, Set<String>> map;
//  private ConcurrentHashMap<String, Set<String>> map;
  public ConsumerThread(Connection connection, MongoClient mongoClient) {
    this.connection = connection;
    this.mongoClient = mongoClient;
    this.map = new HashMap<>();
  }

  @Override
  public void run() {
    try {
      final Channel channel = connection.createChannel();

      // Durable, Non-exclusive(Can be shared across different channels),
      // Non-autoDelete, classic queue.
      channel.queueDeclare(QUEUE_NAME, true, false, false, new HashMap<>(Map.of("x-queue-type", "classic")));
      channel.queueBind(QUEUE_NAME, RMQConnectionInfo.EXCHANGE_NAME, "");   // No Routing key in fanout mode

      // Max one message per consumer (to guarantee even distribution)
      channel.basicQos(BATCH_UPDATE_SIZE);
      System.out.println(" [*] MatchConsumer Thread waiting for messages. To exit press CTRL+C");

      final int[] batch_cnt = {0};
      DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), "UTF-8");
        // Store data into a thread-safe hashmap
        SwipeDetails swipeDetails = gson.fromJson(message, SwipeDetails.class);
        String swiperId = swipeDetails.getSwiper();
        String swipeeId = swipeDetails.getSwipee();

        if (swipeDetails.getDirection() == SwipeDetails.RIGHT) {
          this.map.putIfAbsent(swiperId, new HashSet<>());
          this.map.get(swiperId).add(swipeeId);
          //TODO: Write(insert or update) to twinder.Matches collection. Batch process??
          // Aggregate 100 swipes, store temprarily in a data structure (map?),
          // then use MongoDB's bulkWrite to update or insert a new document into twinder.Matches collectio
        }

        batch_cnt[0] ++;

        if (batch_cnt[0] == BATCH_UPDATE_SIZE) {
          // TODO: Write to DB


          // Manual Acknowledgement in Batch
          channel.basicAck(delivery.getEnvelope().getDeliveryTag(), true);
          this.map = new HashMap<>();
        }

        System.out.println( "Callback thread ID = " + Thread.currentThread().getId() + " Received '" + message + "'");
      };

      // No autoAck, to ensure that Consumer only acknowledges Queue after the message got processed succesfully.
      // Nolocal
      // IsNot exclusive. If exclusive, queues may only be accessed by the current connection. (But we want Another Consumer to access this queue as well)
      // server-generated consumerTag
      channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});

    } catch (IOException e) {
      Logger.getLogger(ConsumerThread.class.getName()).log(Level.SEVERE, null, e);
    }
  }
}
