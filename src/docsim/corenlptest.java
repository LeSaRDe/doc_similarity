package docsim;

import java.util.*;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.*;
import java.lang.StringBuffer;
import java.lang.Exception;
import java.util.regex.*;
import java.util.concurrent.ThreadLocalRandom;

import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.trees.*;
import edu.stanford.nlp.semgraph.*;
import edu.stanford.nlp.parser.nndep.*;
import edu.stanford.nlp.util.*;
import edu.stanford.nlp.trees.*;
import edu.stanford.nlp.trees.GrammaticalStructure.Extras;


class corenlptest
{
//    static public void main(String[] argv)
//    {
//        ArrayList<String> l_tasks = new ArrayList(Arrays.asList(Constants.ANN_TASKS.split("\\|")));
//        System.out.println(String.join("|", l_tasks));
//        System.out.println("[DBG]: AnnotateTextTask: run():" + l_tasks.toString());
//        if(l_tasks.contains(Constants.ANN_TASK_TAG))
//        {
//            System.out.println("tagged text is set.");
//        }
//        if(l_tasks.contains(Constants.ANN_TASK_CON))
//        {
//            System.out.println("constituency is set.");
//        }
//        if(l_tasks.contains(Constants.ANN_TASK_DEP))
//        {
//            System.out.println("depparse phrases are set.");
//        }
//    }



    static public void main(String[] argv)
    {
        String in_txt = "There is no dog wrestling and hugging.";
        Properties m_props = new Properties();
        m_props.setProperty("annotators", "tokenize, ssplit, pos, parse");
        m_props.setProperty("depparse.model", "edu/stanford/nlp/models/parser/nndep/english_UD.gz");
        StanfordCoreNLPClient m_client = new StanfordCoreNLPClient(m_props,
                Constants.CORENLP_SERV_HOSTNAME,
                //m_curr_port_seed + 9000,
                Constants.CORENLP_SERV_PORT,
                Constants.CORENLP_CLIENT_THREAD);
        Annotation annodoc = new Annotation(in_txt);
        m_client.annotate(annodoc);
        CoreDocument coredoc = new CoreDocument(annodoc);

        Properties depparse_prop = new Properties();
//        depparse_prop.setProperty("depparse.model", "edu/stanford/nlp/models/parser/nndep/english_UD.gz");
        depparse_prop.setProperty("depparse.extradependencies", "MAXIMAL");
        DependencyParser depparser = DependencyParser.loadFromModelFile("edu/stanford/nlp/models/parser/nndep/english_UD.gz", depparse_prop);


        ArrayList<CoreSentence> m_sentences = new ArrayList<CoreSentence>(coredoc.sentences());
        for(CoreSentence sent : m_sentences)
        {
//            SemanticGraph dep_parse_tree = sent.dependencyParse();
//            for(SemanticGraphEdge dep_edge : dep_parse_tree.edgeIterable())
//            {
//                System.out.println(dep_edge.getGovernor().word() + ":" + dep_edge.getDependent().word() + ":"
//                        + dep_edge.getRelation().toString());
//            }
            CoreMap sent_coremap = sent.coreMap();
            GrammaticalStructure depparse_tree = depparser.predict(sent_coremap);
//            List<TypedDependency> l_deps = depparse_tree.typedDependenciesEnhancedPlusPlus();
            List<TypedDependency> l_deps = depparse_tree.typedDependenciesCCprocessed(Extras.MAXIMAL);
            for(TypedDependency dep : l_deps)
            {
                System.out.println(dep.gov().word() + ":" + dep.dep().word() + ":" + dep.reln().toString());
            }
        }
    }
}