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
        m_corenlp.getDecomposedSentences();
        m_corenlp.getConstituentTrees();
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
        String sent_str = String.join("|", l_sentences.stream().map(desent->desent.toTaggedSentenceString()).collect(Collectors.toList()));
        String tree_str = String.join("|", l_sentences.stream().map(desent->desent.getPrunedTree(true).toString()).collect(Collectors.toList()));
        //System.out.println("[DBG]: parse_trees = " + tree_str);
        m_in_utrec.settaggedtext(sent_str);
        m_in_utrec.setparsetrees(tree_str);
        m_utin.addUpdatedAnnotateTextRec(m_in_utrec);
        m_corenlp.shutdownCoreNLPClient();
        m_corenlp = null;
        //System.out.println("[DBG]: UserTextTask one record ready!");
    }
}
