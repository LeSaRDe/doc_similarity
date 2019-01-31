package docsim;

import java.util.*;
import java.lang.*;
import java.io.*;
import java.sql.*;

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
            String query_str = String.format("SELECT doc_id FROM %s", Constants.ANNTXT_DB_TB_DOCS);
            Statement st = null;
            ResultSet rs = null;
            st = db_conn.createStatement();
            rs = st.executeQuery(query_str);
            ArrayList<String> docids = new ArrayList<String>();
            while(rs.next())
            {
                docids.add(rs.getString("doc_id"));
            }
            System.out.println("[INF]: Total number of docs = " + docids.size());
            db_conn.close();
            //DBG
            //System.out.println(docids.toString());

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
            System.out.println("[ERR]: " + e.toString());
        } 
    }
}
