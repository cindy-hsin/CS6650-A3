package assignment3.servlet.rmqpool;
import assignment3.config.constant.RMQConnectionInfo;
import com.rabbitmq.client.Channel;
import java.io.IOException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simple RabbitMQ channel pool based on a BlockingQueue implementation
 * Ref: https://github.com/gortonator/foundations-of-scalable-systems/blob/main/Ch7/rmqpool/RMQChannelPool.java
 */
public class RMQChannelPool {
  private final BlockingQueue<Channel> pool;    // used to store and distribute channels
  private int capacity;       // fixed size pool
  private RMQChannelFactory factory;    // used to create channels

  public RMQChannelPool(int capacity, RMQChannelFactory factory) {
    this.capacity = capacity;
    this.pool = new LinkedBlockingQueue<>(capacity);
    this.factory = factory;
    for (int i = 0; i < capacity; i++) {
      Channel channel;
      try {
        channel = factory.create();
        // Declare a durable exchange
        // NOTE: Declare exchanges when creating channels in Channel pool,
        // instead of declaring them when sending each msg, will be much faster.
        channel.exchangeDeclare(RMQConnectionInfo.EXCHANGE_NAME, "direct", true);
        this.pool.put(channel);
      } catch (IOException | InterruptedException ex) {
        Logger.getLogger(RMQChannelPool.class.getName()).log(Level.SEVERE, null, ex);
      }
    }
  }

  public Channel borrowObject() throws IOException {
    try {
      return this.pool.take();
    } catch (InterruptedException e) {
      throw new RuntimeException("Error: no channels available" + e.toString());
    }
  }

  public void returnObject(Channel channel) throws Exception {
    if (channel != null) {
      this.pool.add(channel);
    }
  }

  public void close() {
    // this.pool.close();   TODO: ?? What to do here?
  }
}
