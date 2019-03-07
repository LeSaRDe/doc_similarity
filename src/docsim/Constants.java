package docsim;

import java.util.*;
import java.lang.*;

class Constants
{
    //static final boolean EN_BABELFY = false;
    //static final String CORENLP_SERV_HOSTNAME = "http://hswlogin1";
    //static final String CORENLP_SERV_HOSTNAME = "http://pegasus1";
    static final String CORENLP_SERV_HOSTNAME = "http://localhost";
    static final int CORENLP_SERV_PORT = 9000;
    static final int CORENLP_CLIENT_THREAD = 12;
    //static final String DB_PATH = "jdbc:sqlite:/hpchome/fcmeng/doc_sim/docsim.db";
	//static final String ANNTXT_DB_CONNSTR = String.format("jdbc:sqlite:/home/%s/workspace/data/lee_bg.db", System.getenv("USER"));
    static final String ANNTXT_DB_CONNSTR = "jdbc:sqlite:/home/fcmeng/workspace/data/msr.db";
    //static final String STOPWORDS_PATH = String.format("/home/%s/workspace/doc_similarity/res/stopwords.txt", System.getenv("USER"));
    static final String STOPWORDS_PATH = "/home/fcmeng/workspace/doc_similarity/res/stopwords.txt";
    //static final String DB_TB_NAME = "tb_docsim";
    static final String ANNTXT_DB_TB_DOCS = "docs";
    //static final String WS_SERVER_HOSTNAME = "pegasus1";
    static final String WS_SERVER_HOSTNAME = "localhost";
    //static final String WS_SERVER_HOSTNAME = "hswlogin1";
    static final String ANN_FILE_PREFIX = "/home/fcmeng/gh_data/ann_ret/ann_ret_";
    static final int ANNTXT_MAX_TASKS = 12;
    static final int ANNTXT_MAX_CACHED = 20;
    static final int ANNTXT_THREAD_POOL_SIZE = 24;
};
