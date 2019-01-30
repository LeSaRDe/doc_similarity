package usersimproj;

import java.util.*;
import java.io.*;
import java.net.*;
import java.util.concurrent.*;

/**
 * This class maintains a thread pool for word similariy queries.
 * The task class is WordSimTask.
 */
class WordSimServer
{
    /**
     * Constants
     */
    // use this constant if we go with FixedThreadPool
    public final int MAX_THREADS = 200;
    public final int SERV_PORT = 8607;
    private final int MAX_TASK = 50;
    
    // FIXED means FixedThreadPool.
    // at any point, at most MAX_THREADS threads will be active
    // processing tasks. if the number of tasks exceeds MAX_THREADS,
    // then some of them have to wait in the queue.
    // CACHED means CachedThreadPool.
    // it has no max limit. threads are created as needed, and previous
    // threads will be reused if available. threads that have not been 
    // used for 60s are terminated and removed from the cache.
    public enum ThreadPoolMode
    {
        FIXED,
        CACHED;
    }

    /**
     * Class Members
     */
    // the thread pool
    //ExecutorService m_pool;
    ThreadPoolExecutor m_pool;
    // the address that this server is bound to.
    private InetAddress m_serv_addr;
    // server socket
    // we use UDP so far. i didn't see any reason
    // that we have to use TCP.
    DatagramSocket m_serv_sock;
    private long m_task_count = 0;
    private boolean m_readytoshutdown = false;

    /**
     * Class Methods
     */
    public WordSimServer(ThreadPoolMode mode)
    {
        // TODO
        // change this address to make this server available from outside.
        try
        {
            m_serv_addr = InetAddress.getByName(UserSimConstants.WS_SERVER_HOSTNAME);
            //m_serv_addr = InetAddress.getLocalHost();
            m_serv_sock = new DatagramSocket(SERV_PORT, m_serv_addr);
            /*
            switch(mode)
            {
                case FIXED:
                {
                    m_pool = Executors.newFixedThreadPool(MAX_THREADS);
                }
                break;

                case CACHED:
                {
                    m_pool = Executors.newCachedThreadPool();
                }
                break;

                default:
                break;
            }
            */
            start();
        }
        catch(Exception e)
        {
            System.out.println("[ERR]: " + e.toString());
        }
    }

    public synchronized DatagramSocket getServSocket()
    {
        return m_serv_sock;
    }
    
    private void start()
    {
        m_readytoshutdown = false;
        m_task_count = 0;
        m_pool = null;
        m_pool = (ThreadPoolExecutor)Executors.newFixedThreadPool(MAX_THREADS);
    }

    public void readyToShutdown()
    {
        m_readytoshutdown = true;
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
                    break;
                }
            }
        }
        catch(Exception e)
        {
            System.out.println("[ERR]: awaitShutdown: " + e.toString());
        }
        finally
        {
            m_pool.purge();
            m_pool = null;
        }
    }

    private void coolDown()
    {
        System.out.println("[DBG]: cool down...");
        awaitShutdown();
        System.out.println("[DBG]: cool down done...");
        start();
    }

    public void go()
    {
        while(true)
        {
            try
            {
                int recvbufsize = m_serv_sock.getReceiveBufferSize();
                byte[] recv_buf = new byte[recvbufsize];
                DatagramPacket recv_pack = new DatagramPacket(recv_buf, recv_buf.length);
                m_serv_sock.receive(recv_pack);
                System.out.println("[DBG]: receive a pack!");
                m_pool.execute(new WordSimTask(this, recv_pack));
                m_task_count += 1;
                /*
                if(m_task_count > MAX_TASK || m_readytoshutdown)
                {
                    coolDown();
                }
                */
            }
            catch(Exception e)
            {
                System.out.println("[ERR]: " + e.toString());
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] argv)
    {
        WordSimServer ws = new WordSimServer(WordSimServer.ThreadPoolMode.FIXED);
        ws.go();
    }
}
