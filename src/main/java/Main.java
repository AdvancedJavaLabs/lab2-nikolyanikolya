import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class Main {
  public static void main(String[] args) throws Exception {
    var startTime = System.currentTimeMillis();
    var lexer = new LexiconSentiment("positive-words.txt", "negative-words.txt");
    int N = Runtime.getRuntime().availableProcessors();
    ExecutorService pool = Executors.newFixedThreadPool(N);
    Set<Long> taskIds = Splitter.split("war_peace_plain.txt", pool);
    var configProvider = new MqConfigProvider();
    ConnectionFactory factory = configProvider.connectionFactory();
    factory.setSharedExecutor(pool);
    Connection aggregatorConnection = factory.newConnection();
    Connection workersConnection = factory.newConnection();
    pool.submit(() -> {
      try {
        Worker.run(N, workersConnection, lexer, args);
      } catch (IOException | TimeoutException e) {
        throw new RuntimeException(e);
      }
    });

    Aggregator.aggregate(aggregatorConnection, taskIds);

    aggregatorConnection.addShutdownListener((e) -> {
      try {
        workersConnection.close();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
      pool.shutdown();
      System.out.printf("total time: %s ms", (System.currentTimeMillis() - startTime));
      System.exit(0);
    });
  }
}
