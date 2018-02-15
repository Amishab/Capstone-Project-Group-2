package verbExtractor;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

import org.json.simple.*;
import org.json.simple.parser.JSONParser;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

public class verbExtractor {
    StanfordCoreNLP pipeline;
    String text;

    public verbExtractor()
    {
        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse");
        this.pipeline = new StanfordCoreNLP(props);

        this.text = this.loadJSON("data/json/articles/caliNewsPaper_senate_sample.json");

    }

    public void extractVerb()
    {
        Annotation document = new Annotation(this.text);
        pipeline.annotate(document);

        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

        for (CoreMap sentence : sentences) {
            // this is the Stanford dependency graph of the current sentence
            SemanticGraph dependencies = sentence.get(SemanticGraphCoreAnnotations.EnhancedPlusPlusDependenciesAnnotation.class);
            System.out.println(dependencies.toString());

            IndexedWord rootVerb = dependencies.getFirstRoot();
            System.out.println("rootverb :" + rootVerb.originalText() + " lemma: " + rootVerb.lemma());
        }

    }

    private String loadJSON(String filename) {
        JSONParser parser = new JSONParser();
        StringBuilder sb = new StringBuilder();

        try {
            FileReader fileReader = new FileReader(filename);
            JSONArray jsonArray = (JSONArray) parser.parse(fileReader);

            Iterator i = jsonArray.iterator();
            while (i.hasNext())
            {
                JSONObject jsonObject = (JSONObject)i.next();
                String news = (String) jsonObject.get("news");

                sb.append(news);
            }

            return sb.toString();

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return null;
   }

    public static void main(String args[]) {
        verbExtractor ve = new verbExtractor();
        ve.extractVerb();
    }
}