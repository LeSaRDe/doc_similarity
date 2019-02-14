package docsim;

import java.util.*;
import java.io.*;
import java.net.*;
import java.util.concurrent.*;

/**
 * This class is a task for computing word similarity.
 */
class WordSimTask implements Runnable
{
    /**
     * Class Members
     */
    // words are just strings
    private String m_word_1;
    private String m_word_2;
    // a synset consists of a list of sense offsets, 
    // obtained from Babelfy, attached with POS.
    // e.g. 12345678n
    private List<String> m_synset_1;
    private List<String> m_synset_2;
    // one of the modes listed in WordSimMode
    private WordSimMode m_mode;
    // ADW instance
    private ADWWrap m_adw;
    // Client received package
    // private DatagramPacket m_c_recv_pack;
    // Client address
    private InetAddress m_c_addr;
    // Client port
    private int m_c_port;
    // Server socket
    private DatagramSocket m_serv_sock;
    private WordSimServer m_server;

    public enum WordSimMode
    {
        WORD_WORD,
        OFFSET_OFFSET,
        POS_POS;
    }

    /**
     * Class Methods
     */
    public WordSimTask(WordSimServer server, String word_1, String word_2)
    {
        if(word_1 == null || word_2 == null)
        {
            System.out.println("[ERR]: Invalid input for WordSimTask!");
        }
        m_word_1 = word_1;
        m_word_2 = word_2;
        m_mode = WordSimMode.WORD_WORD;
        m_adw = new ADWWrap();
        m_c_addr = null;
        m_c_port = -1;
        m_server = server;
    }

    public WordSimTask(WordSimServer server, List<String> synset_1, List<String> synset_2)
    {
        if(synset_1 == null || synset_2 == null || synset_1.size() == 0 || synset_2.size() == 0)
        {
            System.out.println("[ERR]: Invalid input for WordSimTask!");
        }
        m_synset_1 = synset_1;
        m_synset_2 = synset_2;
        m_mode = WordSimMode.OFFSET_OFFSET;
        m_adw = new ADWWrap();
        m_c_addr = null;
        m_c_port = -1;
        m_server = server;
    }

    // this is the constructor truly in use by WordSimServer
    public WordSimTask(WordSimServer server, DatagramPacket c_pack)
    {
        m_server = server;
        parseRecvPack(c_pack);
        //m_mode = WordSimMode.OFFSET_OFFSET; 
        m_mode = WordSimMode.POS_POS; 
        m_adw = new ADWWrap();
    }

    // for synset input:
    // mode#offset11+offset12+...#offset21+offset22+...
    // e.g. oo#12345678n+23456789n#34567891n
    // for tagged word input:
    // e.g. tt#like:v#love:v
    // tags include 'n'-noun, 'v'-verb, 'a'-adj, and 'r'-adv
    // any two types can be compared
    // mode:
    // 1. oo: offset vs offset
    // 2. ww: word vs word
    // 3. ow: offset vs word
    // 4. wo: word vs offset
    // 5. tt: tagged word vs tagged word
    private void parseRecvPack(DatagramPacket c_pack)
    {
        m_c_addr = c_pack.getAddress();
        m_c_port = c_pack.getPort();
        //System.out.println("[DBG]: client: " + m_c_addr.toString() + ":" + m_c_port);
        String recv_str = new String(c_pack.getData(), 0, c_pack.getLength());
        /*if(recv_str.equals("client_done"))
        {
            m_server.readyToShutdown();
            return;
        }*/
        //System.out.println("[DBG]: recv_str = " + recv_str);
        String[] recv_str_array = recv_str.split("#");
       // System.out.println("[DBG]: recv_str_array = " + Arrays.toString(recv_str_array));
        if(recv_str_array[0].equals("oo"))
        {
            m_mode = WordSimMode.OFFSET_OFFSET; 
            m_synset_1 = Arrays.asList(recv_str_array[1].split("\\+")); 
            m_synset_2 = Arrays.asList(recv_str_array[2].split("\\+")); 
            System.out.println("[DBG]: " + "synset1 = " + m_synset_1.toString() + ":" + "synset2 = " + m_synset_2.toString());
        }
        else if(recv_str_array[0].equals("ww"))
        {
            m_mode = WordSimMode.WORD_WORD; 
            m_word_1 = recv_str_array[1]; 
            m_word_2 = recv_str_array[2]; 
        }
        else if(recv_str_array[0].equals("ow"))
        {}
        else if(recv_str_array[0].equals("wo"))
        {}
        else if(recv_str_array[0].equals("tt"))
        {
            m_mode = WordSimMode.POS_POS;
            m_word_1 = recv_str_array[1].replace(':', '#'); 
            m_word_2 = recv_str_array[2].replace(':', '#');
            System.out.println("[DBG]: " + "word1 = " + m_word_1 + " : " + "word2 = " + m_word_2);
        }

    }


    private void sendResultToClient(Double sim)
    {
        try
        {
            String send_str = Double.toString(sim);
            byte[] send_buf = send_str.getBytes();
            System.out.println("[DBG]: " + "send_str = " + send_str);
            DatagramPacket send_pack = new DatagramPacket(send_buf, send_buf.length, m_c_addr, m_c_port);
            m_server.getServSocket().send(send_pack);
        }
        catch(Exception e)
        {
            System.out.println("[ERR]: " + e.toString());
        }
    }

    public void run()
    {
        switch(m_mode)
        {
            case WORD_WORD:
            //TODO
            //implement this part with NASARI
            break;

            case OFFSET_OFFSET:
            {
                Double ret = m_adw.getWordPairSimilarity(m_synset_1, m_synset_2);
                sendResultToClient(ret);
            }
            break;

            case POS_POS:
            {
                Double ret = m_adw.getWordPairSimilarity(m_word_1, m_word_2);
                sendResultToClient(ret);
            }

            default:
            break;
        }
    }


}
