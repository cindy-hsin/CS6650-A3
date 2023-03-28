package assignment3.matchconsumer;

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
import com.mongodb.client.model.WriteModel;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Filter;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

// MatchConsumerThread
public class ConsumerThread implements Runnable{

  private static final String QUEUE_NAME = "write_to_db";
  private static final int BATCH_UPDATE_SIZE = 2;
  private static final Gson gson = new Gson();
  private Connection connection;
  private MongoClient mongoClient;

  private Map<Integer, Set<Integer>> matchesMap = new HashMap<>();
  private Map<Integer, int[]> statsMap = new HashMap<>();


//  private ConcurrentHashMap<String, Set<String>> map;
  public ConsumerThread(Connection connection, MongoClient mongoClient) {
    this.connection = connection;
    this.mongoClient = mongoClient;
  }

  @Override
  public void run() {
    try {
      final Channel channel = connection.createChannel();

      // Durable, Non-exclusive(Can be shared across different channels),
      // Non-autoDelete, classic queue.
      channel.queueDeclare(QUEUE_NAME, true, false, false, new HashMap<>(Map.of("x-queue-type", "classic")));
      channel.queueBind(QUEUE_NAME, RMQConnectionInfo.EXCHANGE_NAME, "write_to_db");

      // Max one message per consumer (to guarantee even distribution)
      channel.basicQos(BATCH_UPDATE_SIZE);
      System.out.println(" [*] MatchConsumer Thread waiting for messages. To exit press CTRL+C");

      // Connect to MongoDB
      MongoDatabase database = mongoClient.getDatabase(MongoConnectionInfo.DATABASE);
      MongoCollection<Document> matchesCollection = database.getCollection(MongoConnectionInfo.MATCH_COLLECTION);
      MongoCollection<Document> statsCollection = database.getCollection(MongoConnectionInfo.STATS_COLLECTION);

      System.out.println("MongoDB connected!");
      // ======= HARD-CODED TEST DB WRITE ====
//      List bulkOps = new ArrayList<>();
//      Bson filter = Filters.eq("_id", 888);
//      Bson initUpdate = Updates.setOnInsert("matches",  new ArrayList<>());
//      Bson addUpdate = Updates.addEachToSet("matches", Arrays.asList(1,3,5,7,9));
//
//       UpdateOneModel<Document> initUpdateModel = new UpdateOneModel<>(filter, initUpdate,
//          new UpdateOptions().upsert(true));
//      UpdateOneModel<Document> addUpdateModel = new UpdateOneModel<>(filter, addUpdate,
//          new UpdateOptions().upsert(false));
//
//      // false -> Ensure that if no document matches the filter, a new document won't be inserted
//      // with the specified value (bc the specified value is not an initial value -> empty list.)
//      // TODO: check if no document matches, will a new document be created with matches == empty array?
//      //  Check if it's okay to drop the initUpdate for an array type field?
//      //
//      bulkOps.add(initUpdateModel);
//      bulkOps.add(addUpdateModel);
//
//      try {
//        BulkWriteResult result = matchesCollection.bulkWrite(bulkOps);
//        System.out.println("thread ID = " + Thread.currentThread().getId() + "\nBulk write to Matches:" +
//            "\ninserted: " + result.getInsertedCount() +
//            "\nupdated: " + result.getModifiedCount() +
//            "\ndeleted: " + result.getDeletedCount() +
//            "\nHashmap id count: " + this.matchesMap.size());
//      } catch (MongoException me) {
//        System.out.println("thread ID = " + Thread.currentThread().getId() + ": Bulk write to Matches failed due to an error: " + me);
//      }


//      List bulkOps = new ArrayList<>();
//      Bson filter = Filters.eq("_id", 888);
//      // setOnInsert: If the document already exists, this field will not be modified.
//      // Only if a new document is inserted as a result of an update operation, will the field be specified the given value.
//      Bson insertIfAbsent = Updates.combine(Updates.setOnInsert("likes", 0),
//          Updates.setOnInsert("dislikes", 0));
//      Bson incUpdate = Updates.combine(
//          Updates.inc("likes", 1),
//          Updates.inc("dislikes", 4));
//      // TODO: check if no document matches, will a new document be created with likes == 0 and dislikes == 0?
//
//      UpdateOneModel <Document> insertIfAbsentModel = new UpdateOneModel<>(filter, insertIfAbsent,
//          new UpdateOptions().upsert(true));
//      UpdateOneModel <Document> incUpdateModel = new UpdateOneModel<>(filter, incUpdate,
//          new UpdateOptions().upsert(false));
//      // false -> Ensure that if no document matches the filter, a new document won't be inserted
//      // with the specified value (bc the specified value is the amount to increment, not an initial value.)
//
//      bulkOps.add(insertIfAbsentModel);
//      bulkOps.add(incUpdateModel);
//
//    try {
//      BulkWriteResult result = statsCollection.bulkWrite(bulkOps);
//      System.out.println("thread ID = " + Thread.currentThread().getId() + "\nBulk write to Stats:" +
//          "\ninserted: " + result.getInsertedCount() +
//          "\nupdated: " + result.getModifiedCount() +
//          "\ndeleted: " + result.getDeletedCount() +
//          "\nHashmap id count: " + this.matchesMap.size());
//    } catch (MongoException me) {
//      System.out.println("thread ID = " + Thread.currentThread().getId() + ": Bulk write to Stats failed due to an error: " + me);
//    }

      // ======= HARD-CODED TEST DB WRITE ====

      final int[] batch_cnt = {0};

      DeliverCallback deliverCallback = (consumerTag, delivery) -> {
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
        System.out.println( "Callback thread ID = " + Thread.currentThread().getId() + " Received '" + message + "'");
        batch_cnt[0] ++;

        if (batch_cnt[0] < BATCH_UPDATE_SIZE) {
          return;
        }

        this.updateMatchesCollection(matchesCollection);
        this.updateStatsCollection(statsCollection);
        // Manual Acknowledgement in Batch
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), true);
        // Reset map and batch_cnt
        this.matchesMap = new HashMap<>();
        this.statsMap = new HashMap<>();

        batch_cnt[0] = 0;
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

  private void updateMatchesCollection(MongoCollection<Document> collection) {
    List bulkOps = new ArrayList<>();
    System.out.println("Matches map size:" + this.matchesMap.size()); //TODO!!! Check matchesMap size

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
      System.out.println("thread ID = " + Thread.currentThread().getId() + "\nBulk write to Matches:" +
          "\ninserted: " + result.getInsertedCount() +
          "\nupdated: " + result.getModifiedCount() +
          "\ndeleted: " + result.getDeletedCount() +
          "\nHashmap id count: " + this.matchesMap.size());
    } catch (MongoException me) {
      System.out.println("thread ID = " + Thread.currentThread().getId() + ": Bulk write to Matches failed due to an error: " + me);
    }
  }

  private void updateStatsCollection(MongoCollection<Document> collection) {
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
      // TODO: check if no document matches, will a new document be created with likes == 0 and dislikes == 0?

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
      System.out.println("thread ID = " + Thread.currentThread().getId() + "\nBulk write to Stats:" +
          "\ninserted: " + result.getInsertedCount() +
          "\nupdated: " + result.getModifiedCount() +
          "\ndeleted: " + result.getDeletedCount() +
          "\nHashmap id count: " + this.matchesMap.size());
    } catch (MongoException me) {
      System.out.println("thread ID = " + Thread.currentThread().getId() + ": Bulk write to Stats failed due to an error: " + me);
    }
  }
}
