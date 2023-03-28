package assignment3.likesconsumer;

import assignment3.config.constant.RMQConnectionInfo;
import assignment3.config.datamodel.SwipeDetails;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

// StatsConsumerThread

public class ConsumerThread implements Runnable{

  private static final String QUEUE_NAME = "likes";
  private static final Gson gson = new Gson();
  private Connection connection;
  private ConcurrentHashMap<String, int[]> map;


  public ConsumerThread(Connection connection, ConcurrentHashMap<String, int[]> map) {
    this.connection = connection;
    this.map = map;
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
      channel.basicQos(1);
      System.out.println(" [*] LikesConsumer Thread waiting for messages. To exit press CTRL+C");

      DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), "UTF-8");
        // Store data into a thread-safe hashmapq
        SwipeDetails swipeDetails = gson.fromJson(message, SwipeDetails.class);
        String swiperId = swipeDetails.getSwiper();

        this.map.putIfAbsent(swiperId, new int[] {0,0});
        if (swipeDetails.getDirection().equals(SwipeDetails.RIGHT)) {
          this.map.get(swiperId)[0] ++;
        } else {
          this.map.get(swiperId)[1] ++;
        }

        // Manual Acknowledgement
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
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
