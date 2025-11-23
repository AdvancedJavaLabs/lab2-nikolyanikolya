import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

import java.util.Properties;

public class NlpProcessor {

  public static void main(String[] args) {
    var text = "Just so it now seems as if we have only to admit the law of\n" +
      "inevitability, to destroy the conception of the soul, of good and evil,\n" +
      "and all the institutions of state and church that have been built up on\n" +
      "those conceptions.\n" +
      "\n" +
      "So too, like Voltaire in his time, uninvited defenders of the law of\n" +
      "inevitability today use that law as a weapon against religion, though\n" +
      "the law of inevitability in history, like the law of Copernicus in\n" +
      "astronomy, far from destroying, even strengthens the foundation on which\n" +
      "the institutions of state and church are erected.\n" +
      "\n" +
      "As in the question of astronomy then, so in the question of history\n" +
      "now, the whole difference of opinion is based on the recognition or\n" +
      "nonrecognition of something absolute, serving as the measure of visible\n" +
      "phenomena. In astronomy it was the immovability of the earth, in history\n" +
      "it is the independence of personality—free will.";

    Properties props = new Properties();
    props.setProperty("annotators", "tokenize,ssplit,parse,sentiment");

    StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
    Annotation annotation = new Annotation(text);
    pipeline.annotate(annotation);

    for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
      String sentiment = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
      System.out.println(sentiment);
    }
  }
}
