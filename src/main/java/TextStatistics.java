import java.util.List;

public record TextStatistics(
  Long wordsCount,
  List<FrequentWord> topNFrequentWords,
  List<String> sortedSentences
) {
}
