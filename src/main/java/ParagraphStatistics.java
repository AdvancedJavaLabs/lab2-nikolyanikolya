import java.util.List;

public record ParagraphStatistics(
  Long taskId,
  Long wordsCount,
  List<FrequentWord> topNFrequentWords,
  List<String> sortedSentences
) {}

