import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class Main {
  public static void main(String[] args) throws Exception {
    Producer.main(args);
    int N = Runtime.getRuntime().availableProcessors();
    ExecutorService pool = Executors.newFixedThreadPool(N);
    for (int i = 0; i < N; i++) {
      pool.submit(() -> {
        try {
          Worker.main(args);
        } catch (IOException | TimeoutException e) {
          throw new RuntimeException(e);
        }
      });
    }
    Aggregator.main(args);
  }
}
