import java.util.List;
import java.util.Map;

public record TextStatistics(
  Long wordsCount,
  List<FrequentWord> topNFrequentWords,
  List<String> sortedSentences,
  Map<String, Long> sentiments,
  List<Replacement> replacements
) {
}
