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

  private static final Integer N_AGGREGATORS = 1;

  public static void main(String[] args) throws IOException, TimeoutException {
    var storage = new Storage();
    var configProvider = new MqConfigProvider();
    ConnectionFactory factory = configProvider.connectionFactory();
    ExecutorService es = Executors.newFixedThreadPool(N_AGGREGATORS);
    Connection conn = factory.newConnection();
    Channel ch = conn.createChannel();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        ch.close();
        conn.close();
        es.shutdown();
      } catch (IOException | TimeoutException e) {
        throw new RuntimeException(e);
      }
    }));

    ch.queueDeclare(AGGREGATOR_QUEUE, true, false, false, null);

    // ограничить количество сообщений, не подтверждённых одновременно
    ch.basicQos(N_AGGREGATORS, true);
    ch.basicQos(1, false);

    System.out.println("Waiting for messages...");
    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      try {
        String body = new String(delivery.getBody(), StandardCharsets.UTF_8);
        System.out.println("Received: " + body);
        storage.save(body);
        ch.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        System.out.println("Processed");
      } catch (Exception e) {
        System.out.println("Failed to process");
        ch.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
      }
    };

    boolean autoAck = false;
    ch.basicConsume(AGGREGATOR_QUEUE, autoAck, deliverCallback, consumerTag -> {
    });
    System.out.println("Aggregator running, press CTRL+C to stop it");
  }
}

