import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LexiconSentiment {

  private final Set<String> positiveWords;
  private final Set<String> negativeWords;

  private static final String WORD_DELIMITERS_REGEX = "[^\\p{L}\\p{N}]+";

  public LexiconSentiment(String positivePath, String negativePath) throws IOException {
    positiveWords = loadWords(positivePath);
    negativeWords = loadWords(negativePath);
  }

  public String analyzeSentiment(String text) {
    String[] tokens = text.toLowerCase().split(WORD_DELIMITERS_REGEX);

    int score = 0;
    for (String token : tokens) {
      if (positiveWords.contains(token)) score++;
      if (negativeWords.contains(token)) score--;
    }
    if (score >= 2) {
      return "Positive";
    } else if (score <= -2) {
      return "Negative";
    } else {
      return "Neutral";
    }
  }

  private Set<String> loadWords(String filePath) throws IOException {
    try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
      return lines
        .map(String::trim)
        .filter(line -> !line.isEmpty())
        .map(String::toLowerCase)
        .collect(Collectors.toSet());
    }
  }
}
