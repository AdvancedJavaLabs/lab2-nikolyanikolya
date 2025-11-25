import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

  public static void main(String[] args) throws Exception {
    var maxN = Runtime.getRuntime().availableProcessors();

    for (int i = 1; i <= maxN; i++) {
      File file = new File(String.format("%s/%s.txt", "time-metrics", i));
      System.setErr(new PrintStream(file));
      launch(i, "war_peace_plain.txt", args);
    }

    // generateData("generated-texts", args);
  }

  public static void launch(int N, String fileName, String[] args) throws Exception {
    var startTime = System.currentTimeMillis();
    var lexer = new LexiconSentiment("positive-words.txt", "negative-words.txt");
    ExecutorService pool = Executors.newFixedThreadPool(N);

    var configProvider = new MqConfigProvider();
    ConnectionFactory factory = configProvider.connectionFactory();
    factory.setSharedExecutor(pool);
    Connection aggregatorConnection = factory.newConnection();
    Connection workersConnection = factory.newConnection();
    aggregatorConnection.addShutdownListener((e) -> {
      try {
        workersConnection.close();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
      pool.shutdown();
    });

    Set<Long> taskIds = Splitter.split(fileName, pool);
    for (int i = 0; i < N; i++) {
      Worker.run(i, workersConnection, lexer, args);
    }
    Aggregator.aggregate(aggregatorConnection, taskIds);

    while (aggregatorConnection.isOpen()) {}
    System.err.printf("total time: %s ms", (System.currentTimeMillis() - startTime));
  }

  private static void generateData(String dirName, String[] args) throws Exception {
    var maxSizeInMb = 64;
    for (int i = 1; i <= maxSizeInMb; i*=2) {
      var file = new File(String.format("%s/%s-MB.txt", dirName, i));
      System.setErr(new PrintStream(String.format("data-metrics/%s-Mb.txt", i)));
      try (
        FileWriter fw = new FileWriter(file);
        FileReader fr = new FileReader("war_peace_plain.txt");
        BufferedWriter bw = new BufferedWriter(fw);
        BufferedReader br = new BufferedReader(fr);
      ) {
        var line = br.readLine();
        var size = 0;
        while (line != null && size < i * 1024 * 1024)  {
          size += line.getBytes(StandardCharsets.UTF_8).length;
          bw.write(line);
          bw.newLine();
          bw.flush();
          line = br.readLine();
        }
        launch(Runtime.getRuntime().availableProcessors(), String.format("%s/%s", dirName, file.getName()), args);
      }
    }
  }
}
