import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class Worker {
  private static final String EXCHANGE = "workers_exchange";
  private static final String QUEUE = "workers_queue";
  private static final String ROUTING_KEY = "task";
  private static final String AGGREGATOR_EXCHANGE = "results_exchange";
  private static final String AGGREGATOR_ROUTING_KEY = "partial_result";

  public static void main(String[] args) throws IOException, TimeoutException {
    var configProvider = new MqConfigProvider();
    ConnectionFactory factory = configProvider.connectionFactory();

    Connection conn = factory.newConnection();
    Channel ch = conn.createChannel();
    ch.basicQos(1);

    ch.exchangeDeclare(EXCHANGE, "direct", true);
    ch.queueDeclare(QUEUE, true, false, false, null);
    ch.queueBind(QUEUE, EXCHANGE, ROUTING_KEY);

    ch.exchangeDeclare(AGGREGATOR_EXCHANGE, "direct", true);

    System.out.printf("Worker %s is waiting for messages...\n", Thread.currentThread().getName());
    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      String body = new String(delivery.getBody(), StandardCharsets.UTF_8);
      AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
        .deliveryMode(2) // persistent
        .contentType("application/json")
        .build();

      try {
        ch.basicPublish(AGGREGATOR_EXCHANGE, AGGREGATOR_ROUTING_KEY, props, delivery.getBody());
        System.out.printf("Task %s was processed by %s\n", body, Thread.currentThread().getName());
        ch.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
      } catch (IOException e) {
        System.out.printf("Failed to process task %s by %s\n", body, Thread.currentThread().getName());
        ch.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
        throw new RuntimeException(e);
      }
    };

    ch.basicConsume(QUEUE, false, deliverCallback, consumerTag -> {});
  }
}

