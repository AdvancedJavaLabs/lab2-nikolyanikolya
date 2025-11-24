import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Aggregator {
  private static final String AGGREGATOR_EXCHANGE = "results_exchange";
  private static final String AGGREGATOR_QUEUE = "results_queue";
  private static final String AGGREGATOR_ROUTING_KEY = "partial_result";
  private static final Integer TOP_N = 50;
  private static final ObjectMapper mapper = new ObjectMapper();

  public static void aggregate(Connection aggregatorConnection, Set<Long> taskIds) throws IOException, TimeoutException {
    var tasksToWait = new HashSet<>(taskIds);
    var storage = new Storage();
    Channel ch = aggregatorConnection.createChannel();

    ch.exchangeDeclare(AGGREGATOR_EXCHANGE, "direct", true);
    ch.queueDeclare(AGGREGATOR_QUEUE, true, false, false, null);
    ch.queueBind(AGGREGATOR_QUEUE, AGGREGATOR_EXCHANGE, AGGREGATOR_ROUTING_KEY);

    var statistics = new AtomicReference<TextStatistics>();

    System.out.println("Aggregator is waiting for messages...");
    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      String body = new String(delivery.getBody(), StandardCharsets.UTF_8);
      var paragraphStatistics = mapper.readValue(body, ParagraphStatistics.class);
      try {
        statistics.set(merge(statistics.get(), paragraphStatistics));
        tasksToWait.remove(paragraphStatistics.taskId());
        ch.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        if (tasksToWait.isEmpty()) {
          storage.save("results", mapper.writeValueAsString(statistics.get()));
          ch.basicCancel(consumerTag);
          if (aggregatorConnection.isOpen()) {
            aggregatorConnection.close();
          }
        }
        System.out.printf("Task %s was processed by Aggregator (thread = %s)\n", paragraphStatistics.taskId(), Thread.currentThread().getName());
      } catch (Exception e) {
        System.out.printf("Failed to proccess task %s by Aggregator\n.", paragraphStatistics.taskId());
        ch.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
        throw new RuntimeException(e);
      }
    };

    ch.basicConsume(AGGREGATOR_QUEUE, false, deliverCallback, consumerTag -> {});
  }

  public static TextStatistics merge(TextStatistics cur, ParagraphStatistics paragraphStatistics) {
    if (cur == null) {
      return new TextStatistics(
        paragraphStatistics.wordsCount(),
        paragraphStatistics.topNFrequentWords(),
        paragraphStatistics.sortedSentences(),
        sentiments(Map.of(), paragraphStatistics.sentiments()),
        paragraphStatistics.replacements()
      );
    }
    var sentences = merge(
      paragraphStatistics.sortedSentences(),
      cur.sortedSentences(),
      Comparator.comparingInt(String::length)
    );
    var wordsCount = cur.wordsCount() + paragraphStatistics.wordsCount();
    var topNFrequentWords = frequentWords(cur, paragraphStatistics);
    var sentiments = sentiments(cur.sentiments(), paragraphStatistics.sentiments());

    return new TextStatistics(wordsCount, topNFrequentWords, sentences, sentiments, cur.replacements());
  }

  public static <T> List<T> merge(List<T> a, List<T> b, Comparator<T> comparator) {
    List<T> merged = new ArrayList<>();
    int i = 0, j = 0;

    while (i < a.size() && j < b.size()) {
      if (comparator.compare(a.get(i), b.get(j)) >= 0) {
        merged.add(a.get(i));
        i++;
      } else {
        merged.add(b.get(j));
        j++;
      }
    }

    while (i < a.size()) {
      merged.add(a.get(i));
      i++;
    }

    while (j < b.size()) {
      merged.add(b.get(j));
      j++;
    }

    return merged;
  }

  private static List<FrequentWord> frequentWords(TextStatistics cur, ParagraphStatistics paragraphStatistics) {
    return Stream.concat(
        paragraphStatistics.topNFrequentWords().stream(),
        cur.topNFrequentWords().stream()
      )
      .collect(
        Collectors.groupingBy(
          FrequentWord::word, Collectors.summingLong(FrequentWord::count)
        ))
      .entrySet()
      .stream()
      .map(entry -> new FrequentWord(entry.getKey(), entry.getValue()))
      .sorted((fw1, fw2) -> fw2.count().compareTo(fw1.count()))
      .limit(TOP_N)
      .toList();
  }

  private static Map<String, Long> sentiments(Map<String, Long> curSentiments, List<String> newSentiments) {
    var paragraphSentiments = newSentiments
      .stream()
      .collect(Collectors.groupingBy(s -> s, Collectors.counting()));

    return Stream.concat(
      paragraphSentiments.entrySet().stream(),
      curSentiments.entrySet().stream()
    ).collect(Collectors.toMap(
      Map.Entry<String, Long>::getKey,
      Map.Entry<String, Long>::getValue,
      Long::sum
    ));
  }
}

