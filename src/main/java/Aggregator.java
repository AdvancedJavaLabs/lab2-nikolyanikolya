import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class Aggregator {
  private static final String AGGREGATOR_EXCHANGE = "results_exchange";
  private static final String AGGREGATOR_QUEUE = "results_queue";
  private static final String AGGREGATOR_ROUTING_KEY = "partial_result";

  public static void main(String[] args) throws IOException, TimeoutException {
    var storage = new Storage();
    var configProvider = new MqConfigProvider();
    ConnectionFactory factory = configProvider.connectionFactory();
    Connection conn = factory.newConnection();
    Channel ch = conn.createChannel();

    ch.exchangeDeclare(AGGREGATOR_EXCHANGE, "direct", true);
    ch.queueDeclare(AGGREGATOR_QUEUE, true, false, false, null);
    ch.queueBind(AGGREGATOR_QUEUE, AGGREGATOR_EXCHANGE, AGGREGATOR_ROUTING_KEY);

    ch.basicQos(1);

    System.out.println("Aggregator is waiting for messages...");
    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      String body = new String(delivery.getBody(), StandardCharsets.UTF_8);
      try {
        storage.save(body);
        ch.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        System.out.printf("Task %s was processed by Aggregator\n", body);
      } catch (Exception e) {
        System.out.printf("Failed to process task %s by Aggregator\n", body);
        ch.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
        throw new RuntimeException(e);
      }
    };

    ch.basicConsume(AGGREGATOR_QUEUE, false, deliverCallback, consumerTag -> {});
    System.out.println("Aggregator running, press CTRL+C to stop it");
  }
}

