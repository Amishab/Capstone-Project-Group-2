package AnalyticsProcessor;

import edu.stanford.nlp.dcoref.CorefChain;
import edu.stanford.nlp.dcoref.CorefCoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations;
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

        HashMap<Tree, String> idMap = new HashMap<>();
        int leafIndex = 1;

        int nodeIdx = 1;
        while (it.hasNext()) {
            Tree curr = it.next();
            String nodeId = "node " + nodeIdx++;

            if (curr.isLeaf()) leafIndex++;
            idMap.put(curr, nodeId);
        }

        System.out.println(idMap.toString());
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

            // this is the parse tree of the current sentence
            Tree tree = sentence.get(TreeCoreAnnotations.TreeAnnotation.class);

            tree.setSpans();

            System.out.println(tree.toString());

            buildTree(tree, tokens);

//            // this is the Stanford dependency graph of the current sentence
//            SemanticGraph dependencies = sentence.get(SemanticGraphCoreAnnotations.EnhancedPlusPlusDependenciesAnnotation.class);
//            System.out.println(dependencies.toString());

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

//        for(String result:results) {
//            System.out.println(result);
//        }
    }
}