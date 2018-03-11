package AnalyticsProcessor;

import edu.stanford.nlp.dcoref.CorefChain;
import edu.stanford.nlp.dcoref.CorefCoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.semgraph.SemanticGraphEdge;
import edu.stanford.nlp.trees.GrammaticalRelation;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations;
import edu.stanford.nlp.trees.UniversalEnglishGrammaticalRelations;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.util.Index;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.neo4j.driver.v1.*;

import javax.xml.bind.SchemaOutputResolver;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;

import static java.lang.System.exit;

import utils.replace_UTF8;

public class AnalyticsProcessor implements AutoCloseable {
    StanfordCoreNLP pipeline;
    ArrayList<NewsArticles> newsArticles;

    String inFileName = "data/json/graphbuilder/test2graph.json";

    private final Driver neo4jDriver;

    private class NewsArticles   {
        String newsArticle;
        String newsID;
        String collectionDate;

        NewsArticles (String news, Long id, String date) {
            newsArticle = news;
            newsID = id.toString();
            collectionDate = date;
        }
    }

    public AnalyticsProcessor()
    {
        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse");
        this.pipeline = new StanfordCoreNLP(props);

        this.newsArticles = new ArrayList<>();

        this.loadJSON(inFileName);

        this.neo4jDriver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "1234"));

        try (Session session = neo4jDriver.session()) {
            session.run("MATCH (n) DETACH DELETE n");
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
                Long newsId = (Long) jsonObject.get("newsId");
                String collectionDate = (String) jsonObject.get("converted_collection_date");

                this.newsArticles.add(new NewsArticles(news, newsId, collectionDate));
            }
            System.out.println("Total articles: " + Integer.toString(this.newsArticles.size()));

        } catch (Exception ex) {
            ex.printStackTrace();
        }
   }

    private void neo4JSemanticGraphBuilder(SemanticGraph dependencies, String docId, int senId,
                                           String collectionDate, String collectionTime) throws IOException {
        List<SemanticGraphEdge> edgeList = dependencies.edgeListSorted();
        ListIterator<SemanticGraphEdge> it = edgeList.listIterator();

        while (it.hasNext()) {
            SemanticGraphEdge edge = it.next();
            String source = edge.getSource().value();
            int sourceIdx = edge.getSource().index();

            String target = edge.getTarget().value();
            int targetIdx = edge.getTarget().index();

            String s_ner  = edge.getSource().ner();
            String t_ner  = edge.getTarget().ner();

            String s_tag  = edge.getSource().tag();
            String t_tag  = edge.getTarget().tag();

            String relation = edge.getRelation().toString();
            relation = relation.replace(":", "_");

            try (Session session = neo4jDriver.session()) {
                session.run(
                        "MERGE (a:"+s_ner+" { word : \"" + source
                        + "\", idx: "+ String.valueOf(sourceIdx)
                        + ", docId : "+ docId
                        + ", senId : " + String.valueOf(senId)
                        + ", Date: \"" + collectionDate + "\""
                        + ", Time: \"" + collectionTime + "\""
                        +", tag : \""+s_tag+"\" }) "
                        + "MERGE (b:"+t_ner+" { word: \"" + target
                        + "\", idx: "+ String.valueOf(targetIdx)
                        + ", docId : "+ docId
                        + ",senId : " + String.valueOf(senId)
                        + ", Date: \"" + collectionDate + "\""
                        + ", Time: \"" + collectionTime  + "\""
                        + ", tag :\"" +t_tag+ "\"})"
                        + "MERGE (a)-[r:dep{type:\""+relation+"\"}]->(b)");
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }

        GraphReducer gr = new GraphReducer(neo4jDriver);
        gr.runRules(docId,senId);

    }

    @Override
    public void close() throws Exception {
        this.neo4jDriver.close();
    }

    private String replaceIllegalChars(String text) {
        try {
            text = replace_UTF8.ReplaceLooklike(text);
            text = text.replace("≦", "<=");
            text = text.replace("≧", ">=");
            text = text.replace("㎡", "m2");
            text = text.replace("ï", "i");
            text = text.replace("˄", "^");
            text = text.replace("˚", " degrees");
            text = text.replace("※", "-");
            text = text.replace("㎲", " microseconds");
            text = text.replace("́s", "'s");
            text = text.replace("╳", "x");
        } catch (IOException e) {
            e.printStackTrace();
        }
        text = text.replace(" - ", "\n\n");
        text = text.replace("- ", "\n\n");
        return text.trim();
    }

    public void build() throws IOException {
        for (NewsArticles newsArticle: newsArticles) {
            String text = newsArticle.newsArticle;
            String docID = newsArticle.newsID;
            String collectionDate = newsArticle.collectionDate.split(" ")[0];
            String collectionTime = newsArticle.collectionDate.split(" ")[1];

            if (text == null || text.isEmpty()) {
                continue;
            }

            ArticleCleaner ac = new ArticleCleaner(text);
            text = ac.clean();

            if (text.isEmpty()) {
                continue;
            }

            Annotation document = new Annotation(text);
            pipeline.annotate(document);

            // these are all the sentences in this document
            // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
            List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

            int sentenceID = 0;
            for (CoreMap sentence : sentences) {
                sentenceID++;

                // this is the Stanford dependency graph of the current sentence
                SemanticGraph dependencies = sentence.get(SemanticGraphCoreAnnotations.EnhancedPlusPlusDependenciesAnnotation.class);

                neo4JSemanticGraphBuilder(dependencies, docID, sentenceID,collectionDate,collectionTime);
//                break;
            }

//            break;
        }

        System.out.println("All done!!!");
    }

    public static void main(String args[]) throws IOException {
        long start = System.currentTimeMillis();
        String starttime = LocalDateTime.now().toString();

        System.out.println("Start: ");

        AnalyticsProcessor ap = new AnalyticsProcessor();
        ap.build();

        // Get elapsed time in milliseconds
        long elapsedTimeMillis = System.currentTimeMillis()-start;

        System.out.println(" Start   : " + starttime
                + "\n End     : "+ LocalDateTime.now() + "\n Elapsed : " + String.valueOf(elapsedTimeMillis/1000) + "s");

        exit(0);
    }
}