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

import java.util.*;

public class AnalyticsProcessor {
    StanfordCoreNLP pipeline;

    public AnalyticsProcessor()
    {
        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
        this.pipeline = new StanfordCoreNLP(props);

    }

    private void buildTree(Tree root, List<CoreLabel> map) {
        Iterator<Tree> it = root.iterator();

        HashMap<String, String> idMap = new HashMap<>();
        int leafIndex = 1;

        int nodeIdx = 1;
        while (it.hasNext()) {
            Tree curr = it.next();

            String nodeId = "node " + nodeIdx++;
            String nodeVal = curr.value();

            idMap.put(nodeId, nodeVal);
        }

        System.out.println(idMap.toString());
    }

    private void buildDependency(SemanticGraph dependencies) {
        IndexedWord rootVerb = dependencies.getFirstRoot();
        System.out.println(rootVerb.toString());

        List<SemanticGraphEdge> outEdges = dependencies.getOutEdgesSorted(rootVerb);

        Set<IndexedWord> nsubjs = dependencies.getChildrenWithReln(rootVerb, UniversalEnglishGrammaticalRelations.NOMINAL_SUBJECT);

        for (IndexedWord nsubj: nsubjs) {
            if (nsubjs.size() == 1) {
                String compound_string = compoundStrings(dependencies, nsubj);

                createNode(compound_string);
            }
        }

        System.out.println("Relation: " + rootVerb.originalText());

        //        Set<IndexedWord> nmods = dependencies.getChildrenWithReln(rootVerb, UniversalEnglishGrammaticalRelations.NOMINAL_MODIFIER);
        Set<IndexedWord> nmods = dependencies.getChildrenWithReln(rootVerb, UniversalEnglishGrammaticalRelations.getNmod("at"));

        for (IndexedWord nmod: nmods)   {
            createNode(nmod.originalText());
        }

        //Set<GrammaticalRelation> rela = dependencies.childRelns(rootVerb);

        //System.out.println(rela.toString());
    }
//
//    private GrammaticalRelation getRelation(SemanticGraph dependencies) {
//
//        Set<GrammaticalRelation> rela = dependencies.childRelns(rootVerb);
//        for (rel <GrammaticalRelation> )
//        rela.iterator().next().getShortName().contains("nmod");
//    }
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
    private String compoundStrings (SemanticGraph dependencies, IndexedWord tmp_word) {
        Set<IndexedWord> compound_nsubjs = dependencies.getChildrenWithReln(tmp_word, UniversalEnglishGrammaticalRelations.COMPOUND_MODIFIER);
        String compound_string = tmp_word.originalText();

        for(IndexedWord compound_nsubj : compound_nsubjs) {
            compound_string =  String.join(" ",compound_nsubj.originalText(), compound_string);
        }

        return compound_string;
    }
    private void createNode(String compound_string)
    {
        // Extract NER of this string
        Annotation doc = new Annotation(compound_string);
        pipeline.annotate(doc);

        String ner_label = getNer(compound_string);

        System.out.println("Node: " + compound_string + " : " + ner_label);
    }
    private void buildDependency(List<SemanticGraphEdge> edgeList) {
        ListIterator<SemanticGraphEdge> it = edgeList.listIterator();
        HashMap<Integer, Integer> map = new HashMap<>();
        int uid = 0;

        while (it.hasNext()) {
            SemanticGraphEdge edge = it.next();
            String source = edge.getSource().value();
            int sourceIdx = edge.getSource().index();
            if (!map.containsKey(new Integer(sourceIdx)))
                map.put(sourceIdx, uid++);

            String target = edge.getTarget().value();
            int targetIdx = edge.getTarget().index();
            if (!map.containsKey(new Integer(targetIdx)))
                map.put(targetIdx, uid++);

            String relation = edge.getRelation().toString();

            System.out.println(source+":-"+relation+"> "+target);
        }
    }

    public void build(String text)
    {
        Annotation document = new Annotation(text);
        pipeline.annotate(document);
        List<String> results = new ArrayList<>();

        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

        for (CoreMap sentence : sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            List<CoreLabel> tokens = sentence.get(CoreAnnotations.TokensAnnotation.class);

            for (CoreLabel tokensInfo : tokens) {
                // this is the text of the token
                String token = tokensInfo.get(CoreAnnotations.TextAnnotation.class);

                // this is the lemma of the token
                String lemma = tokensInfo.get(CoreAnnotations.LemmaAnnotation.class);

                // this is the POS tag of the token
                String pos = tokensInfo.get(CoreAnnotations.PartOfSpeechAnnotation.class);

                // this is the NER label of the token
                String ner = tokensInfo.get(CoreAnnotations.NamedEntityTagAnnotation.class);

                results.add("Token: " + token + "; Lemma: " + lemma + "; POS: " + pos + "; NER:" + ner + "\n");
            }

            for (String result : results) {
                System.out.println(result);
            }

//            // this is the parse tree of the current sentence
//            Tree tree = sentence.get(TreeCoreAnnotations.TreeAnnotation.class);
//            tree.setSpans();
//
//            System.out.println(tree.toString());
//
//            buildTree(tree, tokens);


            // this is the Stanford dependency graph of the current sentence
            SemanticGraph dependencies = sentence.get(SemanticGraphCoreAnnotations.EnhancedPlusPlusDependenciesAnnotation.class);
            System.out.println(dependencies.toString());

            buildDependency(dependencies);

//            List<SemanticGraphEdge> list = dependencies.edgeListSorted();

            List<SemanticGraphEdge> list = dependencies.outgoingEdgeList(dependencies.getFirstRoot());

            this.buildDependency(list);

        }

        // This is the coreference link graph
        // Each chain stores a set of mentions that link to each other,
        // along with a method for getting the most representative mention
        // Both sentence and token offsets start at 1!
//        Map<Integer, CorefChain> graph =
//                document.get(CorefCoreAnnotations.CorefChainAnnotation.class);
//
//        System.out.println(graph.toString());
    }

    public static void main(String args[]) {
        String text = "Barack Obama delivered a speech at UCSD.";

        AnalyticsProcessor ap = new AnalyticsProcessor();


        ap.build(text);
    }
}