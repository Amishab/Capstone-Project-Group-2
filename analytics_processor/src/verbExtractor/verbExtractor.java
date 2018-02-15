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
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class verbExtractor {
    StanfordCoreNLP pipeline;
    ArrayList<String> newsArticles;
    JSONArray verbList = new JSONArray();
    String inFileName = "data/json/articles/caliNewsPaper_senate_sample.json";
    String outFileName = "data/json/articles/caliNewsPaper_verbList.json";

    public verbExtractor()
    {
        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse");
        this.pipeline = new StanfordCoreNLP(props);

        this.newsArticles = new ArrayList<>();

        this.loadJSON(inFileName);

    }

    public void extractVerb()
    {
        for (String text: newsArticles) {
//            Annotation document = new Annotation(String.join(" ", newsArticles));
            Annotation document = new Annotation(text);
            pipeline.annotate(document);

            // these are all the sentences in this document
            // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
            List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

            for (CoreMap sentence : sentences) {
                // this is the Stanford dependency graph of the current sentence
                SemanticGraph dependencies = sentence.get(SemanticGraphCoreAnnotations.EnhancedPlusPlusDependenciesAnnotation.class);
//                System.out.println(dependencies.toString());

                IndexedWord rootVerb = dependencies.getFirstRoot();
                if (rootVerb.ner().equals("O")) {
                    System.out.println("rootverb :" + rootVerb.originalText() + " lemma: " + rootVerb.lemma());

                    JSONObject obj = new JSONObject();
                    obj.put("verb", rootVerb.originalText());
                    obj.put("lemma", rootVerb.lemma());

                    verbList.add(obj);
                }
            }
        }

        try {

            FileWriter file = new FileWriter(outFileName);
            file.write(verbList.toJSONString());
            file.flush();
            file.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void loadJSON(String filename) {
        JSONParser parser = new JSONParser();

        try {
            FileReader fileReader = new FileReader(filename);
            JSONArray jsonArray = (JSONArray) parser.parse(fileReader);

            Iterator i = jsonArray.iterator();

            while (i.hasNext())
            {
                JSONObject jsonObject = (JSONObject)i.next();
                String news = (String) jsonObject.get("news");

                this.newsArticles.add(news);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
   }

    public static void main(String args[]) {
        verbExtractor ve = new verbExtractor();
        ve.extractVerb();
    }
}