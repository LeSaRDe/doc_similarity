package docsim;

import java.util.*;
import java.lang.*;
import java.io.*;
import java.sql.*;
import java.util.stream.*;

import edu.stanford.nlp.trees.*;
import edu.stanford.nlp.ling.*;

class AnnotateTextEntry
{
    static public void main(String[] args)
    {
        try
        {
            // we read in all doc_id's here then send them to AnnotateText to
            // be annotated in parallel.
            Connection db_conn = DriverManager.getConnection(Constants.ANNTXT_DB_CONNSTR);
            ArrayList<String> l_tasks = new ArrayList(Arrays.asList(Constants.ANN_TASKS.split("\\|")));
            ArrayList<String> l_where_clauses = new ArrayList<String>();
            if(l_tasks.contains(Constants.ANN_TASK_TAG))
            {
                l_where_clauses.add("tagged_text is NULL");
            }
            if(l_tasks.contains(Constants.ANN_TASK_CON))
            {
                l_where_clauses.add("parse_trees is NULL");
            }
            if(l_tasks.contains(Constants.ANN_TASK_DEP))
            {
                l_where_clauses.add("depparse_trees is NULL");
            }
            String where_clause = String.join(" OR ", l_where_clauses);
            String query_str = String.format("SELECT doc_id FROM %s WHERE " + where_clause + " ORDER BY doc_id", Constants.ANNTXT_DB_TB_DOCS);
            System.out.println("[DBG]: AnnotateTextEntry:main(): query_str = " + query_str);

//            String query_str = String.format("SELECT doc_id FROM %s WHERE parse_trees is NULL ORDER BY doc_id", Constants.ANNTXT_DB_TB_DOCS);
            //String query_str = String.format("SELECT doc_id FROM %s ORDER BY doc_id LIMIT 2", Constants.ANNTXT_DB_TB_DOCS);

            // DBG
            //String query_str = String.format("SELECT pre_ner FROM docs where doc_id = 'sci.crypt/15823'");
            //String test_text = null;

            Statement st = null;
            ResultSet rs = null;
            st = db_conn.createStatement();
            rs = st.executeQuery(query_str);
            ArrayList<String> docids = new ArrayList<String>();
            while(rs.next())
            {
                docids.add(rs.getString("doc_id"));

                // DBG
                //test_text = rs.getString("pre_ner");
            }
            System.out.println("[INF]: Total number of docs = " + docids.size());
            db_conn.close();
            //DBG
            //System.out.println(docids.toString());
            //CoreNLPWrap corenlp = new CoreNLPWrap(test_text, true);
            //corenlp.getDecomposedSentences();
            //corenlp.getConstituentTrees();
            //List<DeSentence> l_sentences = corenlp.getDeSentences();
            //String sent_str = String.join("|", l_sentences.stream().map(desent->desent.toTaggedSentenceString()).collect(Collectors.toList()));
            //String tree_str = String.join("|", l_sentences.stream().map(desent->desent.getPrunedTree(true).toString()).collect(Collectors.toList()));
            //System.out.println("sent_str:");
            //System.out.println(sent_str);
            //System.out.println("");
            //System.out.println("tree_str:");
            //System.out.println(tree_str);

            // Annotate all text specified by docids
            AnnotateText anntxt = new AnnotateText(Constants.ANNTXT_DB_CONNSTR, docids);
            anntxt.processTextFromDB();
            anntxt.allDone();
/*            
            if (UserSimConstants.ANN_DB_OR_FILE == 0)
            {
                UserTextIn uti = null;
                uti = new UserTextIn(UserSimConstants.DB_PATH);
                uti.processUserTextFromDB(user_id, t_start, t_end);
                uti.allDone();
            }
            else if (UserSimConstants.ANN_DB_OR_FILE == 1)
            {
                UserTextInFile utif = null;
                utif = new UserTextInFile(UserSimConstants.DB_PATH, 
                        UserSimConstants.ANN_FILE_PREFIX + user_id);
                utif.processUserTextFromDB(user_id, t_start, t_end);
                utif.allDone();
            }
            System.out.println("[DBG]: " + user_id + " is done!");
*/
        }
        catch(Exception e)
        {
            System.out.println("[ERR]: AnnotateTextEntry:main(): " + e.toString());
        } 
    }
}
