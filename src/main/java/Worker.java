import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class Worker {
  private static final String EXCHANGE = "workers_exchange";
  private static final String QUEUE = "workers_queue";
  private static final String ROUTING_KEY = "task";
  private static final String AGGREGATOR_EXCHANGE = "results_exchange";
  private static final String AGGREGATOR_ROUTING_KEY = "partial_result";
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final String WORD_DELIMITERS_REGEX = "[^\\p{L}\\p{N}]+";
  private static final String SENTENCES_DELIMITERS_REGEX = "[.?!]\\s*";
  private static final Integer TOP_N = 10;

  public static void main(String[] args) throws IOException, TimeoutException {
    var configProvider = new MqConfigProvider();
    ConnectionFactory factory = configProvider.connectionFactory();

    Connection conn = factory.newConnection();
    Channel ch = conn.createChannel();
    ch.basicQos(1);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        ch.close();
        conn.close();
      } catch (IOException | TimeoutException e) {
        throw new RuntimeException(e);
      }
    }));

    ch.exchangeDeclare(EXCHANGE, "direct", true);
    ch.queueDeclare(QUEUE, true, false, false, null);
    ch.queueBind(QUEUE, EXCHANGE, ROUTING_KEY);

    ch.exchangeDeclare(AGGREGATOR_EXCHANGE, "direct", true);

    System.out.printf("Worker %s is waiting for messages...\n", Thread.currentThread().getName());
    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      String body = new String(delivery.getBody(), StandardCharsets.UTF_8);
      var task = mapper.readValue(body, Task.class);
      var text = task.text();
      var sentences = Arrays.stream(text.split(SENTENCES_DELIMITERS_REGEX))
        .sorted(Comparator.comparingInt(String::length).reversed())
        .toList();
      var words = task.text().split(WORD_DELIMITERS_REGEX);
      var wordsCount = Arrays.stream(words).count();
      var wordFrequencyMap = new HashMap<String, Long>();
      Arrays.stream(words).forEach(w -> {
        wordFrequencyMap.compute(w, (key, oldValue) -> oldValue == null ? 1 : oldValue + 1);
      });
      var topNFrequentWords = wordFrequencyMap.entrySet().stream().sorted((w1, w2) -> Long.compare(w2.getValue(), w1.getValue()))
        .limit(TOP_N)
        .map(entry -> new FrequentWord(entry.getKey(), entry.getValue()))
        .toList();

      var paragraphStatistics = new ParagraphStatistics(task.id(), wordsCount, topNFrequentWords, sentences);

      AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
        .deliveryMode(2)
        .contentType("application/json")
        .build();

      try {
        ch.basicPublish(
          AGGREGATOR_EXCHANGE,
          AGGREGATOR_ROUTING_KEY,
          props,
          mapper.writeValueAsBytes(paragraphStatistics)
        );
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

