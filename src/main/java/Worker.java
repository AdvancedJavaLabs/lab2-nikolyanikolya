import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class Worker {
  private static final String QUEUE = "workers_queue";
  private static final String AGGREGATOR_EXCHANGE = "results_exchange";
  private static final String AGGREGATOR_QUEUE = "results_queue";
  private static final String AGGREGATOR_ROUTING_KEY = "partial_result";

  private static final Integer N_WORKERS = Runtime.getRuntime().availableProcessors();

  public static void main(String[] args) throws IOException, TimeoutException {
    var configProvider = new MqConfigProvider();
    ConnectionFactory factory = configProvider.connectionFactory();
    ExecutorService es = Executors.newFixedThreadPool(N_WORKERS);
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

    ch.queueDeclare(QUEUE, true, false, false, null);
    ch.exchangeDeclare(AGGREGATOR_EXCHANGE, "direct", true);
    ch.queueDeclare(AGGREGATOR_QUEUE, true, false, false, null);
    ch.queueBind(AGGREGATOR_QUEUE, AGGREGATOR_EXCHANGE, AGGREGATOR_ROUTING_KEY);

    // ограничить количество сообщений, не подтверждённых одновременно
    ch.basicQos(N_WORKERS, true);
    ch.basicQos(1, false);

    System.out.println("Waiting for messages...");
    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      String body = new String(delivery.getBody(), StandardCharsets.UTF_8);
      System.out.println("Received: " + body);
      es.submit(() -> {
        // симуляция обработки
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
          .deliveryMode(2) // persistent
          .contentType("application/json")
          .build();

        try {
          ch.basicPublish(AGGREGATOR_EXCHANGE, AGGREGATOR_ROUTING_KEY, props, delivery.getBody());
          System.out.println("Processed");
          ch.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        } catch (IOException e) {
          System.out.println("Failed to process");
          throw new RuntimeException(e);
        }
      });
    };

    boolean autoAck = false;
    ch.basicConsume(QUEUE, autoAck, deliverCallback, consumerTag -> {
    });
    System.out.println("Workers running, press CTRL+C to stop them");
  }
}

