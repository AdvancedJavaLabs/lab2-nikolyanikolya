import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Splitter {
  private static final String SENTENCE_END_REGEX = "[.?!]\\s*";
  private static final Pattern SENTENCE_END = Pattern.compile(SENTENCE_END_REGEX);

  public static Set<Long> split() throws Exception {
    boolean previousSentenceUnfinished = false;
    ArrayList<String> sentences = new ArrayList<>();
    int N = 1;
    Set<Long> taskIds = new HashSet<>();

    try (BufferedReader br = Files.newBufferedReader(Path.of("war_peace_plain-test.txt"), UTF_8)) {
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
            if (previousSentenceUnfinished) {
              sentences.set(sentences.size() - 1, String.join(" ", sentences.getLast(), s));
            } else {
              sentences.add(s);
            }
            if (sentences.size() >= N && SENTENCE_END.matcher(s).find()) {
              taskIds.add(Producer.submit(String.join(".", sentences)));
              sentences.clear();
            }
          }
          previousSentenceUnfinished = containsUnfinishedSentences;
          line = br.readLine();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    return taskIds;
  }
}
