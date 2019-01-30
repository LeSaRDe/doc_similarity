package usersimproj;

import java.util.*;
import java.lang.*;

class UserSimConstants
{
    static final boolean EN_BABELFY = false;
    //static final String CORENLP_SERV_HOSTNAME = "http://hswlogin1";
    //static final String CORENLP_SERV_HOSTNAME = "http://pegasus1";
    static final String CORENLP_SERV_HOSTNAME = "http://localhost";
    static final int CORENLP_SERV_PORT = 9000;
    static final int CORENLP_CLIENT_THREAD = 10;
    //static final String DB_PATH = "jdbc:sqlite:/hpchome/fcmeng/doc_sim/docsim.db";
    static final String DB_PATH = "jdbc:sqlite:/home/fcmeng/gh_data/clean_text.db";
    static final String STOPWORDS_PATH = "/home/fcmeng/workspace/doc_clustering_proj/doc_similarity/res/stopwords.txt";
    //static final String DB_TB_NAME = "tb_docsim";
    static final String DB_TB_NAME = "tb_user_text_full";
    //static final String WS_SERVER_HOSTNAME = "pegasus1";
    static final String WS_SERVER_HOSTNAME = "discovery1";
    //static final String WS_SERVER_HOSTNAME = "hswlogin1";
    // 0: DB, 1: FILE
    static final int ANN_DB_OR_FILE = 1;
    static final String ANN_FILE_PREFIX = "/home/fcmeng/gh_data/ann_ret/ann_ret_";
};
