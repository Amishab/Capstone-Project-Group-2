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
import java.io.*;
import java.time.LocalDateTime;
import java.util.*;

import static java.lang.System.exit;

import utils.replace_UTF8;
import utils.Log;

public class AnalyticsProcessor implements AutoCloseable {
    StanfordCoreNLP pipeline;
    ArrayList<NewsArticles> newsArticles;
    GraphReducer gr;



    int totalSentences;
    int totalArticles;
    int skippedArticles;
    Log log;

    String inFileName = "data/json/graphbuilder/test2graph.json";
    //String inFileName = "data/json/graphbuilder/Final_filteredNews_1.json";

    private final Driver neo4jDriver;

    private class NewsArticles   {
        String newsArticle;
        String newsID;
        String collectionDate;
        String source;

        NewsArticles (String news, Long id, String date, String src) {
            newsArticle = news;
            newsID = id.toString();
            collectionDate = date;
            source = src;
        }
    }

    public AnalyticsProcessor() throws FileNotFoundException {
        this.log = new Log();
        log.add(new File(new File("c://temp//"),"debug_log.txt"));

        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse");
        this.pipeline = new StanfordCoreNLP(props);

        this.newsArticles = new ArrayList<>();
        this.totalArticles = 0;
        this.totalSentences = 0;
        this.skippedArticles = 0;


        this.loadJSON(inFileName);

        this.neo4jDriver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "1234"));

        try (Session session = neo4jDriver.session()) {
            session.run("MATCH (n) DETACH DELETE n");
            this.gr = new GraphReducer(neo4jDriver);

        }
        catch (Exception e) {
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
               // String news = (String) jsonObject.get("news");
                String news = (String) jsonObject.get("newsText");

                Long newsId = (Long) jsonObject.get("newsId");
                String collectionDate = (String) jsonObject.get("converted_collection_date");
                String source = (String) jsonObject.get("src");

                this.newsArticles.add(new NewsArticles(news, newsId, collectionDate, source));
            }
            System.out.println("Total articles: " + Integer.toString(this.newsArticles.size()));
            log.info("Total articles: " + Integer.toString(this.newsArticles.size()));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
   }

    private void neo4JSemanticGraphBuilder(SemanticGraph dependencies, String docId, int senId,
                                           String collectionDate, String collectionTime, String src) throws IOException {
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
                        + ", source: \"" + src + "\""
                        +", tag : \""+s_tag+"\" }) "
                        + "MERGE (b:"+t_ner+" { word: \"" + target
                        + "\", idx: "+ String.valueOf(targetIdx)
                        + ", docId : "+ docId
                        + ",senId : " + String.valueOf(senId)
                        + ", Date: \"" + collectionDate + "\""
                        + ", Time: \"" + collectionTime  + "\""
                        + ", source: \"" + src + "\""
                        + ", tag :\"" +t_tag+ "\"})"
                        + "MERGE (a)-[r:dep{type:\""+relation+"\"}]->(b)");
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }


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
            String source = newsArticle.source;
            String collectionDate = newsArticle.collectionDate.split(" ")[0];
            String collectionTime = newsArticle.collectionDate.split(" ")[1];

            if (text == null || text.isEmpty()) {
                skippedArticles++;
                log.error("Skipped Article ID: " + docID);
                continue;
            }

            long start = System.currentTimeMillis();
            ArticleCleaner ac = new ArticleCleaner(text);
            text = ac.clean();

            if (text.isEmpty()) {
                skippedArticles++;
                log.error("Skipped Article ID: " + docID);
                continue;
            }

            try {
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

                    neo4JSemanticGraphBuilder(dependencies, docID, sentenceID, collectionDate, collectionTime, source);
                    totalSentences++;
//                break;
                }

                totalArticles++;
//            break;

                // Get elapsed time in milliseconds
                long elapsedTimeMillis = System.currentTimeMillis() - start;

                log.info("\n Article ID: " + docID +
                        "\n Time taken: " + (float) elapsedTimeMillis / 1000 + "s" +
                        "\n processed Articles: " + totalArticles +
                        "\n processed Sentences: " + totalSentences +
                        "\n %completed: " + (float) totalArticles / this.newsArticles.size() * 100 +
                        "\n skipped: " + skippedArticles);
            } catch (Exception e) {
                log.error("Article ID: " + docID);
                log.error(e);
            }
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