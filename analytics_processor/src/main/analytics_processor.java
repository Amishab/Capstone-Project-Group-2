package main;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static edu.stanford.nlp.util.Characters.isPunctuation;

public class analytics_processor {


    public static void main(String args[]) {
        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma, ner");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        String text = "President Trump delivered a speech in UCSD; it was very good";

        Annotation document = new Annotation(text);
        pipeline.annotate(document);
        List<String> results = new ArrayList<>();

        List<CoreLabel> tokens = document.get(CoreAnnotations.TokensAnnotation.class);
        for (CoreLabel tokensInfo : tokens) {
            String token = tokensInfo.get(CoreAnnotations.TextAnnotation.class);
            String lemma = tokensInfo.get(CoreAnnotations.LemmaAnnotation.class);
            String pos = tokensInfo.get(CoreAnnotations.PartOfSpeechAnnotation.class);
            String ner = tokensInfo.get(CoreAnnotations.NamedEntityTagAnnotation.class);

            //String dependencies = tokensInfo.get(CoreAnnotations.DependentsAnnotation.class);

        }

        for(String result:results) {
            System.out.println(result);
        }
    }
}