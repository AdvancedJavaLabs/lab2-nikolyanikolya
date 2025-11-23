import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
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
  private static final Integer TOP_N = 50;

  public static void run(int N, Connection conn, LexiconSentiment lexer, String[] args) throws IOException, TimeoutException {
    var replacements = new ArrayList<Replacement>();
    for (int i = 0; i < args.length - 1; i++) {
      replacements.add(new Replacement(
        String.format("\\b(%s|%s)\\b", args[i], capitalized(args[i])),
        args[i + 1]
      ));
    }
    Channel ch = conn.createChannel();
    ch.basicQos(1);

    ch.exchangeDeclare(EXCHANGE, "direct", true);
    ch.queueDeclare(QUEUE, true, false, false, null);
    ch.queueBind(QUEUE, EXCHANGE, ROUTING_KEY);

    ch.exchangeDeclare(AGGREGATOR_EXCHANGE, "direct", true);

    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      String body = new String(delivery.getBody(), StandardCharsets.UTF_8);
      var task = mapper.readValue(body, Task.class);
      var text = task.text();
      for (var r: replacements) {
        text = text.replaceAll(r.from(), r.to());
      }
      var sentences = Arrays.stream(text.split(SENTENCES_DELIMITERS_REGEX))
        .sorted(Comparator.comparingInt(String::length).reversed())
        .toList();
      var words = task.text().split(WORD_DELIMITERS_REGEX);
      var wordsCount = Arrays.stream(words).count();

      var topNFrequentWords = Arrays.stream(words)
        .collect(Collectors.groupingBy(s -> s, Collectors.counting()))
        .entrySet()
        .stream()
        .map(entry -> new FrequentWord(entry.getKey(), entry.getValue()))
        .sorted((w1, w2) -> Long.compare(w2.count(), w1.count()))
        .limit(TOP_N)
        .toList();

      var sentiment = lexer.analyzeSentiment(text);
      // var sentiments = NlpProcessor.analyze(text);
      var paragraphStatistics = new ParagraphStatistics(
        task.id(),
        wordsCount,
        topNFrequentWords,
        sentences,
        List.of(sentiment),
        replacements
      );
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
        System.out.printf("Task %s was processed by %s\n", task.id(), Thread.currentThread().getName());
        ch.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
      } catch (IOException e) {
        System.out.printf("Failed to process task %s by %s\n", task.id(), Thread.currentThread().getName());
        ch.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
        throw new RuntimeException(e);
      }
    };

    ch.basicConsume(QUEUE, false, deliverCallback, consumerTag -> {});
    System.out.printf("Worker %s is waiting for messages...\n", Thread.currentThread().getName());
  }

  private static String capitalized(String source) {
    return source.substring(0, 1).toUpperCase() + source.substring(1);
  }
}

