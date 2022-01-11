package docsim;

import java.io.*;
import java.util.*;
import java.sql.*;
import java.util.stream.*;

class AnnotateTextTask extends Thread
{
    private Connection m_ref_db_conn = null;
    private AnnotateTextRec m_in_utrec = null;
    private AnnotateText m_utin = null;
    private CoreNLPWrap m_corenlp = null;

    public AnnotateTextTask(AnnotateText utin, Connection ref_db_conn, AnnotateTextRec in_utrec)
    {
        m_utin = utin;
        m_ref_db_conn = ref_db_conn;
        m_in_utrec = in_utrec;
        //m_corenlp = new CoreNLPWrap(m_in_utrec.getprenertext(), true);
        // we use the constructor of CoreNLPWrap below for 20news18828.
        m_corenlp = new CoreNLPWrap(m_in_utrec.getprenertext());
    }

    public void setAnnotateTextRec(AnnotateTextRec in_utrec)
    {
        m_in_utrec = in_utrec;
    }

    public void run()
    {
        List<DeSentence> l_sentences = m_corenlp.getDeSentences();
        /*
         * If we don't use Babelfy, then we don't even compile Babelfy
           or BabelWrap.

        if(UserSimConstants.EN_BABELFY)
        {
            BabelWrap bw = new BabelWrap();
            for(DeSentence sent : l_sentences)
            {
                bw.getSynsets(sent);
            }
        }
        */
        ArrayList<String> l_tasks = new ArrayList(Arrays.asList(Constants.ANN_TASKS.split("\\|")));
        System.out.println("[DBG]: AnnotateTextTask: run():" + l_tasks.toString());
        if(l_tasks.contains(Constants.ANN_TASK_TAG))
        {
            m_corenlp.getDecomposedSentence s();
            String sent_str = String.join("|", l_sentences.stream().map(desent->desent.toTaggedSentenceString()).collect(Collectors.toList()));
            m_in_utrec.settaggedtext(sent_str);
            System.out.println("tagged text is set.");
        }
        if(l_tasks.contains(Constants.ANN_TASK_CON))
        {
            m_corenlp.getConstituentTrees();
            String tree_str = String.join("|", l_sentences.stream().map(desent->desent.getPrunedTree(true).toString()).collect(Collectors.toList()));
            m_in_utrec.setparsetrees(tree_str);
            System.out.println("constituency is set.");
        }
        if(l_tasks.contains(Constants.ANN_TASK_DEP))
        {
            m_corenlp.getDependencyTrees();
            String dep_phrases_str = String.join("|", l_sentences.stream().map(desent->desent.getDepPhrasesString()).collect(Collectors.toList()));
            m_in_utrec.setdepphrases(dep_phrases_str);
            System.out.println("depparse phrases are set.");
        }

        m_utin.addUpdatedAnnotateTextRec(m_in_utrec);
        m_corenlp.shutdownCoreNLPClient();
        m_corenlp = null;
        //System.out.println("[DBG]: UserTextTask one record ready!");
    }
}
