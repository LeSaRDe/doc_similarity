package docsim;

import java.io.*;
import java.util.*;
import java.sql.*;
import java.util.concurrent.*;
import java.util.stream.*;
import java.lang.Thread;

//import org.json.simple.*;
//import org.json.simple.parser.*;


class AnnotateText
{
    /**
     * Class Members
     */
    private Connection m_db_conn = null;;
    private List<AnnotateTextRec> m_l_utrec = null;
    private List<AnnotateTextTask> m_l_utask = null;
    private ThreadPoolExecutor m_pool;
    private long m_task_count = 0;
    private Object m_db_lock = null;

    private List<String> m_l_docids = null;
    // m_done_count and m_total_count are used for progress tracking
    private int m_done_count = 0;
    private int m_total_count = 0;

    /**
     * Class Methods
     */
    // read text from input db
    public AnnotateText(String db_conn_str, ArrayList<String> docids)
    {
        if(docids == null || docids.size() ==0)
        {
            System.out.println("[ERR:] No input doc_id!");
            return;
        }
        m_total_count = docids.size();
        m_l_docids = docids;
        m_db_lock = new Object();
        try
        {
            synchronized(m_db_lock)
            {
                m_db_conn = DriverManager.getConnection(db_conn_str);
                m_db_conn.setAutoCommit(false);
            }
        }
        catch(Exception e)
        {
            System.out.println("[ERR]: AnnotateText: " + e.toString());
        }
        m_l_utrec = Collections.synchronizedList(new ArrayList<AnnotateTextRec>());
        m_l_utask = new ArrayList<AnnotateTextTask>();
        start();
    }

    private void start()
    {
        m_task_count = 0;
        m_pool = null;
        m_pool = (ThreadPoolExecutor)Executors.newFixedThreadPool(Constants.ANNTXT_THREAD_POOL_SIZE);
    }

    public void addUpdatedAnnotateTextRec(AnnotateTextRec utrec)
    {
        if(utrec == null)
        {
            return;
        }

        //synchronized(m_l_utrec)
        synchronized(m_db_lock)
        {
            m_l_utrec.add(utrec);
            //System.out.println("[DBG]: one rec added in");
        }
    }

    public void awaitShutdown()
    {
        m_pool.shutdown();
        try
        {
            while(true)
            {
                if(m_pool.awaitTermination(60, TimeUnit.SECONDS))
                {
                    //System.out.println("[DBG]: timeout commit...");
                    commitAnnotateTextRecs(Constants.ANNTXT_MAX_CACHED);
                    break;
                }
            }
            //System.out.println("[DBG]: final commit...");
            commitAnnotateTextRecs(0);
        }
        catch(Exception e)
        {
            System.out.println("[ERR]: awaitShutdown: " + e.toString());
        }
        finally
        {
            for(AnnotateTextTask ut : m_l_utask)
            {
                ut = null;
            }
            m_pool.purge();
            m_pool = null;
        }
        m_l_utask.clear();
    }

    public void shutdownDB()
    {
        if (m_db_conn != null)
        {
            try
            {
                m_db_conn.close();
            }
            catch(Exception e)
            {
                System.out.println("[ERR]: " + e.toString());
            }
        }
    }

    public void allDone()
    {
        awaitShutdown();
        shutdownDB();
    }

    public void processTextFromDB()
    {
        String query_str = null;
        Statement st = null;
        ResultSet rs = null;

        for(int i = 0; i < m_l_docids.size(); i++)
        {
            query_str = String.format("SELECT pre_ner FROM %s WHERE (doc_id = '%s')", Constants.ANNTXT_DB_TB_DOCS, m_l_docids.get(i));
            try
            {
                synchronized(m_db_lock)
                {
                    st = m_db_conn.createStatement();
                    rs = st.executeQuery(query_str);
                }
                System.out.println("[DBG]: current doc_id = " + m_l_docids.get(i));
                AnnotateTextTask new_task = new AnnotateTextTask(this, m_db_conn,
                            new AnnotateTextRec(m_l_docids.get(i), rs.getString("pre_ner"), null, null));
                m_l_utask.add(new_task);
                m_pool.execute(new_task);
                m_task_count += 1;
                if(m_task_count > Constants.ANNTXT_MAX_TASKS)
                {
                    System.out.println("[DBG]: One batch is nearly done!");
                    awaitShutdown();
                    start();
                }
                rs.close();
                st.closeOnCompletion();
            }
            catch(Exception e)
            {
                try
                {
                    rs.close();
                    st.close();
                }
                catch(Exception err)
                {
                    System.out.println("[ERR]: " + err.toString());
                }
                System.out.println("[ERR]: " + e.toString());
            }
        }
    }

    private void commitAnnotateTextRecs(int max_cached)
    {
        //synchronized(m_l_utrec)
        //synchronized(m_db_lock)
        //{
        //System.out.println("[DBG]: m_l_utrec.size() = " + m_l_utrec.size());
        if(m_l_utrec.size() > max_cached)
        {
            //System.out.println("[DBG]: Enter commit...");
            ArrayList<Integer> l_rm = new ArrayList<Integer>();
            String update_str = "UPDATE " + Constants.ANNTXT_DB_TB_DOCS + " SET tagged_text = ?, parse_trees = ? WHERE doc_id = ?";
            try
            (
                PreparedStatement st = m_db_conn.prepareStatement(update_str);
            )
            {
                synchronized(m_db_lock)
                {
                    //System.out.println("[DBG]: prepare sql:" + st);
                    for(AnnotateTextRec utc : m_l_utrec)
                    {
                        st.setString(1, utc.gettaggedtext());
                        st.setString(2, utc.getparsetrees());
                        st.setString(3, utc.getdocid());
                        st.executeUpdate();

                        l_rm.add(m_l_utrec.indexOf(utc));
                        System.out.println("[DBG]: commit rec: " + utc.gettaggedtext() + ":" + utc.getparsetrees() + ":" + utc.getdocid());
                    }
                    System.out.println("[DBG]: commit to DB...");
                    m_db_conn.commit();
                }
            }
            catch(Exception e)
            {
                System.out.println("[ERR]: commitUserTextRecs: " + e.toString());
            }

            Set<Integer> hs = new HashSet<Integer>();
            hs.addAll(l_rm);
            l_rm.clear();
            l_rm.addAll(hs);
            Collections.sort(l_rm, Collections.reverseOrder());

            m_done_count += l_rm.size();
            System.out.printf("[INF]: %d%% annotation is done. \r\n", 100*m_done_count/m_total_count);

            for(Integer i_rm : l_rm)
            {
                //System.out.println("[DBG]: i_rm = " + i_rm.intValue());
                m_l_utrec.remove(i_rm.intValue());
            }
        }
        //}
    }
}
