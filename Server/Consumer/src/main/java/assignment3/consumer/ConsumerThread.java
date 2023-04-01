package assignment3.consumer;

import static assignment3.config.constant.LoadTestConfig.BATCH_UPDATE_SIZE;

import assignment3.config.constant.MongoConnectionInfo;
import assignment3.config.constant.RMQConnectionInfo;
import assignment3.config.datamodel.SwipeDetails;
import com.google.gson.Gson;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

// MatchConsumerThread
public class ConsumerThread implements Runnable{

  private static final String QUEUE_NAME = "write_to_db";

  private static final Gson gson = new Gson();
  private Connection connection;
  private MongoClient mongoClient;

  private Map<Integer, Set<Integer>> matchesMap = new HashMap<>();
  private Map<Integer, int[]> statsMap = new HashMap<>();

  private final int[] batch_cnt = {0};
  private MongoCollection<Document> matchesCollection;

  private MongoCollection<Document> statsCollection;

  private Channel rmqChannel;
  private Delivery lastNackDelivery;
  private Long lastDeliveryTime;


//  private ConcurrentHashMap<String, Set<String>> map;
  public ConsumerThread(Connection connection, MongoClient mongoClient) {
    this.connection = connection;
    this.mongoClient = mongoClient;

    try {
      // Connect to RMQ
      this.rmqChannel = connection.createChannel();

      // Connect to MongoDB
      MongoDatabase database = mongoClient.getDatabase(MongoConnectionInfo.DATABASE);
      this.matchesCollection = database.getCollection(MongoConnectionInfo.MATCH_COLLECTION);
      this.statsCollection = database.getCollection(MongoConnectionInfo.STATS_COLLECTION);
    } catch (IOException e) {
      Logger.getLogger(ConsumerThread.class.getName()).log(Level.SEVERE, null, e);
    }
  }


  @Override
  public void run() {
    try {
      // Durable, Non-exclusive(Can be shared across different channels),
      // Non-autoDelete, classic queue.
      this.rmqChannel.queueDeclare(QUEUE_NAME, true, false, false, new HashMap<>(Map.of("x-queue-type", "classic")));
      this.rmqChannel.queueBind(QUEUE_NAME, RMQConnectionInfo.EXCHANGE_NAME, "write_to_db");

      // Max one message per consumer (to guarantee even distribution)
      this.rmqChannel.basicQos(BATCH_UPDATE_SIZE);
      System.out.println(" [*] Consumer Thread " + Thread.currentThread().getName() + " waiting for messages. To exit press CTRL+C");

      System.out.println("Consumer Thread gets Mongo collection!");

      DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        this.lastDeliveryTime = System.currentTimeMillis();

        String message = new String(delivery.getBody(), "UTF-8");
        // Store data into a thread-safe hashmap
        SwipeDetails swipeDetails = gson.fromJson(message, SwipeDetails.class);
        System.out.println("dir:" + swipeDetails.getDirection());
        Integer swiperId = Integer.valueOf(swipeDetails.getSwiper());
        Integer swipeeId = Integer.valueOf(swipeDetails.getSwipee());

        this.statsMap.putIfAbsent(swiperId, new int[] {0,0});
        if (swipeDetails.getDirection().equals(SwipeDetails.RIGHT)) {
          this.matchesMap.putIfAbsent(swiperId, new HashSet<>());
          this.matchesMap.get(swiperId).add(swipeeId);
          this.statsMap.get(swiperId)[0] ++;
        } else {
          this.statsMap.get(swiperId)[1] ++;
        }
        System.out.println( "Callback thread Name = " + Thread.currentThread().getName() + " Received '" + "swiperId: "+ swiperId + " swipeeId: " + swipeeId);
        this.batch_cnt[0] ++;

        if (this.batch_cnt[0] < BATCH_UPDATE_SIZE) {
          this.lastNackDelivery = delivery;
          return;
        }

        this.batchUpdateOperation(delivery);
      };

      // No autoAck, to ensure that Consumer only acknowledges Queue after the message got processed succesfully.
      // Nolocal
      // IsNot exclusive. If exclusive, queues may only be accessed by the current connection. (But we want Another Consumer to access this queue as well)
      // server-generated consumerTag
      this.rmqChannel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});


      // Periodically Check if there are remaining NAcked msg (which are fewer than
      // BATCH_UPDATE_SIZE and therefore cannot be written to DB), and write them to DB.
//      new java.util.Timer().scheduleAtFixedRate(
//          new java.util.TimerTask() {
//            @Override
//            public void run() {
//              if (lastDeliveryTime !=null && (System.currentTimeMillis() - lastDeliveryTime) > 10000 &&
//              lastNackDelivery != null) {
//                // If this channel hasn't received msg from RMQ for more thatn 10s,
//                // flush the remaining msgs to DB
//                batchUpdateOperation(lastNackDelivery);
//                System.out.println("==== Flushed Thread Name = "
//                    + Thread.currentThread().getName() + " ====");
//              }
//            }
//          }, 200000, 5000 // 10sec
//      );

    } catch (IOException e) {
      Logger.getLogger(ConsumerThread.class.getName()).log(Level.SEVERE, null, e);
    }
  }


  private void batchUpdateOperation(Delivery batchLastDelivery) {
    try {
      this.updateMatchesCollection(this.matchesCollection);
      this.updateStatsCollection(this.statsCollection);
      // Manual Acknowledgement in Batch
      this.rmqChannel.basicAck(batchLastDelivery.getEnvelope().getDeliveryTag(), true);
      // Reset map and batch_cnt
      this.matchesMap = new HashMap<>();
      this.statsMap = new HashMap<>();

      this.batch_cnt[0] = 0;
      this.lastNackDelivery = null;
      System.out.println(
          " ************** Batch Updated " + BATCH_UPDATE_SIZE + " to DB! Thread Name = "
              + Thread.currentThread().getName() + " **************");
    } catch (IOException e) {
      Logger.getLogger(ConsumerThread.class.getName()).log(Level.SEVERE, null, e);
    }
  }
  private void updateMatchesCollection(MongoCollection<Document> collection) {
    List bulkOps = new ArrayList<>();
    System.out.println("Matches map size:" + this.matchesMap.size());

    // Edge case: if none of the swipe directions is right(<--> like <--> match).
    // then there is no match at all.
    // So matchesMap will be empty -> bulkOps will be empty -> DB write error
    if (this.matchesMap.size() == 0) {
      return;
    }

    for (Map.Entry<Integer, Set<Integer>> entry: this.matchesMap.entrySet()) {
      Integer swiperId = entry.getKey();
      Set<Integer> matches = entry.getValue();

      Bson filter = Filters.eq("_id", swiperId);
      Bson initUpdate = Updates.setOnInsert("matches",  new ArrayList<>());
      Bson addUpdate = Updates.addEachToSet("matches", new ArrayList<>(matches));

      UpdateOneModel<Document> initUpdateModel = new UpdateOneModel<>(filter, initUpdate,
          new UpdateOptions().upsert(true));
      UpdateOneModel<Document> addUpdateModel = new UpdateOneModel<>(filter, addUpdate,
          new UpdateOptions().upsert(false));
      // false -> Ensure that if no document matches the filter, a new document won't be inserted
      // with the specified value (bc the specified value is not an initial value -> empty list.)

      bulkOps.add(initUpdateModel);
      bulkOps.add(addUpdateModel);
    }
    System.out.println("maps size:" + matchesMap.size() + " " + statsMap.size());


    try {
      BulkWriteResult result = collection.bulkWrite(bulkOps);
      System.out.println("thread Name = " + Thread.currentThread().getName() + "\nBulk write to Matches:" +
          "\ninserted: " + result.getInsertedCount() +
          "\nupdated: " + result.getModifiedCount() +
          "\ndeleted: " + result.getDeletedCount() +
          "\nHashmap id count: " + this.matchesMap.size());
    } catch (MongoException me) {
      System.out.println("thread Name = " + Thread.currentThread().getName() + ": Bulk write to Matches failed due to an error: " + me);
    }
  }

  private void updateStatsCollection(MongoCollection<Document> collection) {
    System.out.println("!! Stats Map: " + this.statsMap.toString());
    List bulkOps = new ArrayList<>();

    for (Map.Entry<Integer, int[]> entry: this.statsMap.entrySet()) {
      Integer swiperId = entry.getKey();
      int[] stats = entry.getValue();
      int likes = stats[0];
      int dislikes = stats[1];

      Bson filter = Filters.eq("_id", swiperId);
      // setOnInsert: If the document already exists, this field will not be modified.
      // Only if a new document is inserted as a result of an update operation, will the field be specified the given value.
      Bson insertIfAbsent = Updates.combine(Updates.setOnInsert("likes", 0),
          Updates.setOnInsert("dislikes", 0));
      Bson incUpdate = Updates.combine(
          Updates.inc("likes", likes),
          Updates.inc("dislikes", dislikes));

      UpdateOneModel <Document> insertIfAbsentModel = new UpdateOneModel<>(filter, insertIfAbsent,
          new UpdateOptions().upsert(true));
      UpdateOneModel <Document> incUpdateModel = new UpdateOneModel<>(filter, incUpdate,
          new UpdateOptions().upsert(false));
      // false -> Ensure that if no document matches the filter, a new document won't be inserted
      // with the specified value (bc the specified value is the amount to increment, not an initial value.)

      bulkOps.add(insertIfAbsentModel);
      bulkOps.add(incUpdateModel);
    }

    try {
      BulkWriteResult result = collection.bulkWrite(bulkOps);
      System.out.println("thread Name = " + Thread.currentThread().getName() + "\nBulk write to Stats:" +
          "\ninserted: " + result.getInsertedCount() +
          "\nupdated: " + result.getModifiedCount() +
          "\ndeleted: " + result.getDeletedCount() +
          "\nHashmap id count: " + this.matchesMap.size());
    } catch (MongoException me) {
      System.out.println("thread Name = " + Thread.currentThread().getName() + ": Bulk write to Stats failed due to an error: " + me);
    }
  }
}
