import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class Aggregator {
  private static final String AGGREGATOR_EXCHANGE = "results_exchange";
  private static final String AGGREGATOR_QUEUE = "results_queue";
  private static final String AGGREGATOR_ROUTING_KEY = "partial_result";
  private static final Integer TOP_N = 10;
  private static final ObjectMapper mapper = new ObjectMapper();

  public static void aggregate(Set<Long> taskIds) throws IOException, TimeoutException {
    var tasksToWait = new HashSet<>(taskIds);
    var storage = new Storage();
    var configProvider = new MqConfigProvider();
    ConnectionFactory factory = configProvider.connectionFactory();
    Connection conn = factory.newConnection();
    Channel ch = conn.createChannel();

    ch.exchangeDeclare(AGGREGATOR_EXCHANGE, "direct", true);
    ch.queueDeclare(AGGREGATOR_QUEUE, true, false, false, null);
    ch.queueBind(AGGREGATOR_QUEUE, AGGREGATOR_EXCHANGE, AGGREGATOR_ROUTING_KEY);

    ch.basicQos(1);
    var statistics = new AtomicReference<TextStatistics>();

    System.out.println("Aggregator is waiting for messages...");
    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      String body = new String(delivery.getBody(), StandardCharsets.UTF_8);
      var paragraphStatistics = mapper.readValue(body, ParagraphStatistics.class);
      statistics.set(merge(statistics.get(), paragraphStatistics));
      tasksToWait.remove(paragraphStatistics.taskId());
      if (tasksToWait.isEmpty()) {
        try {
          storage.save(mapper.writeValueAsString(statistics.get()));
          ch.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
          System.out.printf("Task %s was processed by Aggregator\n", body);
        } catch (Exception e) {
          System.out.printf("Failed to process task %s by Aggregator\n", body);
          ch.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
          throw new RuntimeException(e);
        }
      }
    };

    ch.basicConsume(AGGREGATOR_QUEUE, false, deliverCallback, consumerTag -> {});
    System.out.println("Aggregator running, press CTRL+C to stop it");
  }

  public static TextStatistics merge(TextStatistics cur, ParagraphStatistics paragraphStatistics) {
    var sentences = merge(
      paragraphStatistics.sortedSentences(),
      cur.sortedSentences(),
      Comparator.comparingInt(String::length),
      -1
    );
    var wordsCount = cur.wordsCount() + paragraphStatistics.wordsCount();
    var topNFrequentWords = merge(
      paragraphStatistics.topNFrequentWords(),
      cur.topNFrequentWords(),
      (fw1, fw2) -> Long.compare(fw2.count(), fw1.count()),
      TOP_N
    );
    return new TextStatistics(wordsCount, topNFrequentWords, sentences);
  }

  public static <T> List<T> merge(List<T> a, List<T> b, Comparator<T> comparator, int takeN) {
    List<T> merged = new ArrayList<>();
    int i = 0, j = 0;

    while (i < a.size() && j < b.size()) {
      if (takeN != -1 && merged.size() >= takeN) {
        return merged;
      }
      if (comparator.compare(a.get(i), b.get(j)) >= 0) {
        merged.add(a.get(i));
        i++;
      } else {
        merged.add(b.get(j));
        j++;
      }
    }

    while (i < a.size()) {
      if (takeN != -1 && merged.size() >= takeN) {
        return merged;
      }
      merged.add(a.get(i));
      i++;
    }

    while (j < b.size()) {
      if (takeN != -1 && merged.size() >= takeN) {
        return merged;
      }
      merged.add(b.get(j));
      j++;
    }

    return merged;
  }
}

