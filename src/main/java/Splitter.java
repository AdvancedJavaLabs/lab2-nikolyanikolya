import com.rabbitmq.client.ConnectionFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Splitter {
  private static final String SENTENCE_END_REGEX = "[.?!]\\s*";
  private static final Pattern SENTENCE_END = Pattern.compile(SENTENCE_END_REGEX);
  private static final Integer SENTENCES_IN_ONE_BLOCK = 1000;

  public static Set<Long> split(String filename, ExecutorService pool, ConnectionFactory factory) throws Exception {
    boolean previousSentenceUnfinished = false;
    ArrayList<String> sentences = new ArrayList<>();
    List<CompletableFuture<Long>> futures = new ArrayList<>();

    try (BufferedReader br = Files.newBufferedReader(Path.of(filename), UTF_8)) {
      var line = br.readLine();
      while (line != null) {
        try {
          var matcher = SENTENCE_END.matcher(line);
          var lineSentences = new ArrayList<String>();
          var endIndex = -1;
          var previousEndIndex = 0;
          while (matcher.find()) {
            endIndex = matcher.end();
            lineSentences.add(line.substring(previousEndIndex, endIndex));
            previousEndIndex = endIndex;
          }
          if (endIndex == -1) {
            lineSentences.add(line);
          } else if (endIndex != line.length()) {
            lineSentences.add(line.substring(endIndex));
          }
          var containsUnfinishedSentences = endIndex != line.length();
          for (var s : lineSentences) {
            if (s.isEmpty()) {
              continue;
            }
            if (previousSentenceUnfinished && !sentences.isEmpty()) {
              sentences.set(sentences.size() - 1, String.join(" ", sentences.getLast(), s));
            } else {
              sentences.add(s);
            }
            if (sentences.size() >= SENTENCES_IN_ONE_BLOCK && SENTENCE_END.matcher(s).find()) {
              var finalSentences = new ArrayList<>(sentences);
              futures.add(CompletableFuture.supplyAsync(() -> {
                try {
                  return Producer.submit(String.join("", finalSentences), factory);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }, pool));
              sentences.clear();
            }
          }
          previousSentenceUnfinished = containsUnfinishedSentences;
          line = br.readLine();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    } catch (IOException e) {
      System.out.println(String.format("IO error: %s", e.getMessage()));
      throw new RuntimeException(e);
    }
    Set<Long> taskIds = new HashSet<>(futures.stream().map(CompletableFuture::join).toList());
    taskIds.add(Producer.submit(String.join(" ", sentences), factory));
    sentences.clear();

    return taskIds;
  }
}
