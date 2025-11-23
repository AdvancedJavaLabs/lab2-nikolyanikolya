import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class NlpProcessor {

  public static List<String> analyze(String text) {

    Properties props = new Properties();
    props.setProperty("annotators", "tokenize,ssplit,parse,sentiment");

    StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
    Annotation annotation = new Annotation(text);
    pipeline.annotate(annotation);
    var sentiments = new ArrayList<String>();
    annotation.get(CoreAnnotations.SentencesAnnotation.class).stream().parallel().forEach(sentence ->{
      sentiments.add(sentence.get(SentimentCoreAnnotations.SentimentClass.class));
    });
    return sentiments;
  }

  public static void main(String[] args) {
    var text = "“Well, Prince, so Genoa and Lucca are now just family estates of the\n" +
      "Buonapartes. But I warn you, if you don’t tell me that this means war,\n" +
      "if you still try to defend the infamies and horrors perpetrated by that\n" +
      "Antichrist—I really believe he is Antichrist—I will have nothing\n" +
      "more to do with you and you are no longer my friend, no longer my\n" +
      "‘faithful slave,’ as you call yourself! But how do you do? I see I\n" +
      "have frightened you—sit down and tell me all the news.”\n" +
      "\n" +
      "It was in July, 1805, and the speaker was the well-known Anna Pávlovna\n" +
      "Schérer, maid of honor and favorite of the Empress Márya Fëdorovna.\n" +
      "With these words she greeted Prince Vasíli Kurágin, a man of high\n" +
      "rank and importance, who was the first to arrive at her reception. Anna\n" +
      "Pávlovna had had a cough for some days. She was, as she said, suffering\n" +
      "from la grippe; grippe being then a new word in St. Petersburg, used\n" +
      "only by the elite.\n" +
      "\n" +
      "All her invitations without exception, written in French, and delivered\n" +
      "by a scarlet-liveried footman that morning, ran as follows:\n" +
      "\n" +
      "“If you have nothing better to do, Count (or Prince), and if the\n" +
      "prospect of spending an evening with a poor invalid is not too terrible,\n" +
      "I shall be very charmed to see you tonight between 7 and 10—Annette\n" +
      "Schérer.”";
    analyze(text);
  }
}
