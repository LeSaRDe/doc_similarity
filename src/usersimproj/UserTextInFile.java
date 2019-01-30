package usersimproj;

import java.io.*;
import java.util.*;
import java.sql.*;
import java.util.concurrent.*;
import java.util.stream.*;
import java.lang.Thread;

//import org.json.simple.*;
//import org.json.simple.parser.*;


class UserTextInFile
{
    /**
     * Class Constants
     */
    public final int MAX_CACHED = 1000;
    public final int MAX_THREADS = 200;
    public final int MAX_TASKS = 500;

    /**
     * Class Members
     */
    private Connection m_db_conn = null;
    private FileOutputStream m_output;
    private List<UserTextRec> m_l_utrec = null;
    private List<UserTextTask> m_l_utask = null;
    private ThreadPoolExecutor m_pool;
    private long m_task_count = 0;
    private Object m_output_lock = null;
    private Object m_db_lock = null;

    /**
     * Class Methods
     */
    // for database input
    public UserTextInFile(String db_conn_str, String file_path)
    {
        m_db_lock = new Object();
        m_output_lock = new Object();
        try
        {
            m_db_conn = DriverManager.getConnection(db_conn_str);
            m_db_conn.setAutoCommit(false);
            m_output = new FileOutputStream(file_path, true);
        }
        catch(Exception e)
        {
            System.out.println("[ERR]: UserTextIn: " + e.toString());
        }
        m_l_utrec = Collections.synchronizedList(new ArrayList<UserTextRec>());
        m_l_utask = new ArrayList<UserTextTask>();
        start();
        //m_pool = Executors.newCachedThreadPool();
    }

    private void start()
    {
        m_task_count = 0;
        m_pool = null;
        m_pool = (ThreadPoolExecutor)Executors.newFixedThreadPool(MAX_THREADS);
    }

    public void addUpdatedUserTextRec(UserTextRec utrec)
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
                    commitUserTextRecs(MAX_CACHED);
                    break;
                }
            }
            //System.out.println("[DBG]: final commit...");
            commitUserTextRecs(0);
        }
        catch(Exception e)
        {
            System.out.println("[ERR]: awaitShutdown: " + e.toString());
        }
        finally
        {
            for(UserTextTask ut : m_l_utask)
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
            finally
            {
                m_db_conn = null;
            }
        }
    }

    public void shutdownOutput()
    {
        if (m_output != null)
        {
            try
            {
                m_output.close();
            }
            catch (Exception e)
            {
                System.out.println("[ERR]: " + e.toString());
            }
            finally
            {
                m_output = null;
            }
        }
    }

    public void allDone()
    {
        awaitShutdown();
        shutdownDB();
        shutdownOutput();
    }

    // this function fetches user text for a given user within a certian time range,
    // and writes back annotated text and parse trees to DB.
    public void processUserTextFromDB(String user_id, String time_s, String time_e)
    {
        String query_str = null;

        if(time_s == null || time_e == null)
        {
            query_str = String.format("SELECT user_id, time, clean_text FROM %s WHERE (user_id = '%s')", UserSimConstants.DB_TB_NAME, user_id);
            // TODO
            // only for test
            //query_str = "select user_id, time, clean_text from tb_user_text_full where (user_id='5VB78863WVpziQEkorI0-Q') and (clean_text like 'Actually%Windows binaries%')";
        }
        else
        {
            query_str = String.format("SELECT user_id, time, clean_text FROM %s WHERE (user_id = '%s') AND (strftime('%%Y-%%m-%%dT%%H:%%M:%%Sz', time) BETWEEN '%s' AND '%s')", UserSimConstants.DB_TB_NAME, user_id, time_s, time_e);
            System.out.println("[DBG]: UserTextInFile: query_str = " + query_str);
        }

        Statement st = null;
        ResultSet rs = null;
        try
        {
            //synchronized(m_db_lock)
            //{
                st = m_db_conn.createStatement();
                rs = st.executeQuery(query_str);
            //}
            while(rs.next())
            {
                //System.out.println("[DBG]: UserTextIn 1");
                UserTextTask new_task = null;
                System.out.println("[DBG]: UserTextInFile: text = " + rs.getString("clean_text"));
                new_task = new UserTextTask(this,
                        new UserTextRec(rs.getString("user_id"), rs.getString("time"), rs.getString("clean_text"), null, null));
                m_l_utask.add(new_task);
                m_pool.execute(new_task);
                m_task_count += 1;
                if(m_task_count > MAX_TASKS)
                {
                    System.out.println("[DBG]: One batch is nearly done!");
                    awaitShutdown();
                    start();
                }
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
        finally
        {
            shutdownDB();
        }
    }

    private void commitUserTextRecs(int max_cached)
    {
        //synchronized(m_l_utrec)
        //synchronized(m_db_lock)
        //{
        //System.out.println("[DBG]: m_l_utrec.size() = " + m_l_utrec.size());
        if(m_l_utrec.size() > max_cached)
        {
            //System.out.println("[DBG]: Enter commit...");
            ArrayList<Integer> l_rm = new ArrayList<Integer>();
            //String update_str = "UPDATE " + UserSimConstants.DB_TB_NAME + " SET tagged_text = ?, parse_trees = ? WHERE user_id = ? AND time = ?";
            try
                //(
                // PreparedStatement st = m_db_conn.prepareStatement(update_str);
                //)
                {
                    synchronized(m_output_lock)
                    {
                        //System.out.println("[DBG]: prepare sql:" + st);
                        for(UserTextRec utc : m_l_utrec)
                        {
                            //st.setString(1, utc.gettaggedtext());
                            //st.setString(2, utc.getparsetrees());
                            //st.setString(3, utc.getuserid());
                            //st.setString(4, utc.gettime());
                            //st.setString(5, utc.getcleantext());
                            //System.out.println("[DBG]: commit sql:" + st);
                            //st.executeUpdate();
                            
                            String rec_to_file = utc.getuserid() + "=" +
                                                 utc.gettime() + "=" +
                                                 utc.gettaggedtext() + "=" +
                                                 utc.getparsetrees() + "\n";
                            m_output.write(rec_to_file.getBytes());


                            l_rm.add(m_l_utrec.indexOf(utc));
                            System.out.println("[DBG]: append rec: " + utc.gettaggedtext() + ":" + utc.getparsetrees() + ":" + utc.getuserid());
                        }
                        System.out.println("[DBG]: append to file...");
                        //m_db_conn.commit();
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

            for(Integer i_rm : l_rm)
            {
                //System.out.println("[DBG]: i_rm = " + i_rm.intValue());
                m_l_utrec.remove(i_rm.intValue());
            }
        }
        //}
    }
}
