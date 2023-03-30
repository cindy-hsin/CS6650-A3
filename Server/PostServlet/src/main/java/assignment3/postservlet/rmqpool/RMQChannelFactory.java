package assignment3.postservlet.rmqpool;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * A simple RabbitMQ channel factory based on the Apache pooling libraries.
 * Ref: https://github.com/gortonator/foundations-of-scalable-systems/blob/main/Ch7/rmqpool/RMQChannelFactory.java
 */
public class RMQChannelFactory extends BasePooledObjectFactory<Channel> {

  private final Connection connection;    // Valid RMQ connection
  private int count;    // used to count created channels for debugging

  public RMQChannelFactory(Connection connection) {
    this.connection = connection;

    this.count = 0;
  }
  @Override
  synchronized public Channel create() throws IOException {
    this.count ++;
    Channel channel = connection.createChannel();
    // Uncomment the line below to validate the expected number of channels are being created
    // System.out.println("Channel created: " + count);
    return channel;
  }

  @Override
  public PooledObject<Channel> wrap(Channel channel) {
    //System.out.println("Wrapping channel");
    return new DefaultPooledObject<>(channel);
  }


  public int getChannelCount() {
    return this.count;
  }

  // for all other methods, the no-op implementation
  // in BasePooledObjectFactory will suffice
}
