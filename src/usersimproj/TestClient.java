package usersimproj;

import java.net.*;
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

import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.trees.*;

public class TestClient
{
    public DatagramSocket m_client_sock;
    public InetAddress m_serv_addr;

    public TestClient()
    {
        try
        {
            m_serv_addr = InetAddress.getLocalHost();
            m_client_sock = new DatagramSocket(8306, m_serv_addr);
        }
        catch (Exception e)
        {
            System.out.println(e.toString());
        }
    }

    public static void main(String[] argv)
    {
        String in_txt = "I really love pizza!";
        Properties m_props = new Properties();
        m_props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse");
        StanfordCoreNLPClient m_client = new StanfordCoreNLPClient(m_props, "http://discovery1", 9000, 10); 
        System.out.println("[DBG]: CoreNLPWrap in_txt = " + in_txt);
        Annotation annodoc = new Annotation(in_txt);
        System.out.println("[DBG]: CoreNLPWrap annotation1");
        m_client.annotate(annodoc);
        System.out.println("[DBG]: CoreNLPWrap annotation2");

        /*
        TestClient tc = new TestClient();
        byte[] buf = new byte[256];

        while(true)
        {
            try
            {
                System.out.println("Client starts...");
                //String send_str = "This is a client!";
                String send_str = "oo" + "#" + "06387980n+06388579n" + "#" + "03588414n";
                byte[] send_buf = send_str.getBytes();
                DatagramPacket send_pack = new DatagramPacket(send_buf, send_buf.length, tc.m_serv_addr, 8607);
                System.out.println("Client sends out a package!");
                tc.m_client_sock.send(send_pack);

                DatagramPacket recv_pack = new DatagramPacket(buf, buf.length);
                tc.m_client_sock.receive(recv_pack);
                System.out.println("Client received a package!");
                String recv_str = new String(recv_pack.getData(), 0, recv_pack.getLength());
                System.out.println(recv_str);
                System.out.println("Client terminates!");
            }
            catch(Exception e)
            {
                System.out.println(e.toString());
            }
        }
        */
    }
}
