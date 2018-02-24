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

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class AnalyticsProcessor {
    StanfordCoreNLP pipeline;
    ArrayList<NewsArticles> newsArticles;
    JSONArray jsonGraphArray = new JSONArray();
    String inFileName = "data/json/articles/caliNewsPaper_senate_sample.json";
    String outFileName = "data/json/articles/caliNewsPaper_senate_sample_graph.json";

    private class NewsArticles   {
        String newsArticle;
        String newsID;

        NewsArticles (String news, Long id) {
            newsArticle = news;
            newsID = id.toString();
        }

    }

    public AnalyticsProcessor()
    {
        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
        this.pipeline = new StanfordCoreNLP(props);

        this.newsArticles = new ArrayList<>();

        this.loadJSON(inFileName);

    }

    private Set<IndexedWord> findNmodChildren(SemanticGraph dependencies, IndexedWord rootVerb)
    {
        Set<IndexedWord> nmodChilds = new HashSet<>();

        for (SemanticGraphEdge edge : dependencies.outgoingEdgeIterable(rootVerb)) {
            if (edge.getRelation().toString().contains("nmod")) {
                nmodChilds.add(edge.getTarget());
            }
        }

        return nmodChilds;
    }

    private String getNer(String compound_string) {

        String ner_label = null;

        // Extract NER of this string
        Annotation doc = new Annotation(compound_string);
        pipeline.annotate(doc);

        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = doc.get(CoreAnnotations.SentencesAnnotation.class);

        for (CoreMap sentence : sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            List<CoreLabel> tokens = sentence.get(CoreAnnotations.TokensAnnotation.class);

            for (CoreLabel tokensInfo : tokens) {
                // this is the NER label of the token
                ner_label = tokensInfo.get(CoreAnnotations.NamedEntityTagAnnotation.class);
            }
        }

        return ner_label;
    }

    private String getAppos(SemanticGraph dependencies, IndexedWord tmp_word) {

        ArrayList<String> appos = new ArrayList<>();

        Set<IndexedWord> apposQualifiers = dependencies.getChildrenWithReln(tmp_word,
                UniversalEnglishGrammaticalRelations.APPOSITIONAL_MODIFIER);

        if(!apposQualifiers.isEmpty())   {

            for(IndexedWord apposQualifier:apposQualifiers)   {
                appos.add(apposQualifier.originalText());

                Set<IndexedWord> dependents = dependencies.getChildren(apposQualifier);
                for(IndexedWord dependent:dependents)   {
                    appos.add(dependent.originalText());
                }
            }
        }

        return appos.toString();
    }
    private JSONObject getAdditionalNodeInfo(SemanticGraph dependencies, IndexedWord tmp_word) {
        JSONObject jsonObj = new JSONObject();

        jsonObj.put("NERLabel", tmp_word.ner());
        jsonObj.put("Qualifier", getAppos(dependencies,tmp_word));

        return jsonObj;
    }

    private JSONArray compoundStrings (SemanticGraph dependencies, Set<IndexedWord> tmp_words) {
        JSONArray compoundJsonArray = new JSONArray();

        ArrayList<String> compoundStringList = new ArrayList<>();

        for (IndexedWord tmp_word : tmp_words) {
            JSONObject compoundJsonObj = new JSONObject();
            Set<IndexedWord> compound_nsubjs = dependencies.getChildrenWithReln(tmp_word,
                    UniversalEnglishGrammaticalRelations.COMPOUND_MODIFIER);
            String compound_string = tmp_word.originalText();

            for (IndexedWord compound_nsubj : compound_nsubjs) {
                compound_string = String.join(" ", compound_nsubj.originalText(), compound_string);
            }
            compoundStringList.add(compound_string);

            compoundJsonObj.put("NodeID", compound_string);
            // append addtional information
            compoundJsonObj.put("MetaInfo",getAdditionalNodeInfo(dependencies, tmp_word));

            compoundJsonArray.add(compoundJsonObj);
        }

        return compoundJsonArray;
    }
    private void createNode(String node, String compound_string) {
        JSONObject jsonobj = new JSONObject();

        System.out.println("Node: " + compound_string);
    }

    private JSONObject buildDependency(SemanticGraph dependencies) {
        JSONObject jsonGraphObj = new JSONObject();
        IndexedWord rootVerb = dependencies.getFirstRoot();

        Set<IndexedWord> nsubjs = dependencies.getChildrenWithReln(rootVerb,
                UniversalEnglishGrammaticalRelations.NOMINAL_SUBJECT);

        if (!nsubjs.isEmpty()) {
            JSONArray compoundJsonArr = compoundStrings(dependencies, nsubjs);
            jsonGraphObj.put("SourceNode", compoundJsonArr);
        }
        else {

        }

        jsonGraphObj.put("Relation: ", rootVerb.lemma());

        Set<IndexedWord> nmodChilds = findNmodChildren(dependencies,rootVerb);
        if (!nmodChilds.isEmpty())   {
            JSONArray compoundJsonArr = compoundStrings(dependencies, nmodChilds);
            jsonGraphObj.put("TargetNode", compoundJsonArr);
        }
        else    {
            Set<IndexedWord> dobjChilds = dependencies.getChildrenWithReln(rootVerb,
                    UniversalEnglishGrammaticalRelations.OBJECT);

            if (!dobjChilds.isEmpty())   {
                JSONArray compoundJsonArr  = compoundStrings(dependencies, dobjChilds);
                jsonGraphObj.put("TargetNode", compoundJsonArr );
            }
        }

        return jsonGraphObj;
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

                this.newsArticles.add(new NewsArticles(news, newsId));
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
   }

    public void build()
    {
        for (NewsArticles newsArticle: newsArticles) {
            String text = newsArticle.newsArticle;
            String docID = newsArticle.newsID;

            Annotation document = new Annotation(text);
            pipeline.annotate(document);

            // these are all the sentences in this document
            // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
            List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

            int sentenceID = 0;
            for (CoreMap sentence : sentences) {
                JSONObject jsonGraphObj;
                sentenceID++;

                // this is the Stanford dependency graph of the current sentence
                SemanticGraph dependencies = sentence.get(SemanticGraphCoreAnnotations.EnhancedPlusPlusDependenciesAnnotation.class);
                //            System.out.println(dependencies.toString());
                jsonGraphObj = buildDependency(dependencies);

                JSONObject jsonIDobj = new JSONObject();
                jsonIDobj.put("DocumentID", docID);
                jsonIDobj.put("SentenceID", sentenceID);

                jsonGraphObj.put("ID",jsonIDobj);


                jsonGraphArray.add(jsonGraphObj);
            }
        }
        // This is the coreference link graph
        // Each chain stores a set of mentions that link to each other,
        // along with a method for getting the most representative mention
        // Both sentence and token offsets start at 1!
//        Map<Integer, CorefChain> graph =
//                document.get(CorefCoreAnnotations.CorefChainAnnotation.class);
//
//        System.out.println(graph.toString());

        try {

            FileWriter file = new FileWriter(outFileName);
            file.write(jsonGraphArray.toJSONString());
            file.flush();
            file.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) {
        AnalyticsProcessor ap = new AnalyticsProcessor();
        ap.build();
    }
}