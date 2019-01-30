package usersimproj;

/**
 * This class will be mainly used for computing word-word similarities
 * over the set of words extracted from two input pieces of text. 
 * 
 * Specifically, we would like to compute the similarity between any given word 
 * from one text and another given word from the other text. 
 * The input for this class is expected to be two text, and the output of 
 * this class should be a symmetric matrix (in a simplified way, i.e. only the 
 * upper triangle half has values), where each entry is a real value 
 * (normalized to [0, 1]) reflecting the similarity between the word from the row
 * and the word from the column. 
 *
 * The general procedure is as follows:
 * - Step 1: Utilize CoreNLP to split each piece of text into a set of sentences, 
 *           then obtain the lemma and the POS tag for each word (exclude stopwords).
 * - Step 2: Construct a BabelfyToken for each word taking its lemma and the POS tag,
 *           then utilize Babelfy to obtain the corresponding synset for this word. 
 * - Step 3: Taking a synset from one text and another synset from the other text, 
 *           compute the word-word similarity by utilizing ADW. Then store the similarities
 *           to a SQLite DB (or just a text file). 
 */
public class WordSim
{
    /**
     * For one user, this class is used for storing the text issued by this user within
     * a certain time range. 
     */
    class UsrTxtRec
    {
        // store raw text
        public String m_raw_txt;
        // id of this user
        public String m_usr_id;
        // start time of the text
        public String m_t_start;
        // end time of the text
        public String m_t_end;
        // the sentences from the raw text
        public ArrayList<String> m_sentences;
    }

    /**
     * Class Members
     */
    // there should be exactly two elements (one for each user) in this list
    private ArrayList<UsrRxtRec> m_l_usr_txt; 
    

	public WordSim()
	{
	    	
	}
	
	public 
}
