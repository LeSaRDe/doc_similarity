package usersimproj;

import java.util.*;
//import it.uniroma1.lcl.babelfy.commons.*;

/**
 * This class is playing a proxy role between CoreNLP and Babel.
 * Since we do need some additional info of a word, such as its
 * lemma and POS tag, for Babel to get precise synset, and we will 
 * prefer to obtaining these info from CoreNLP, then it would be 
 * convenient to have such a proxy class in between. 
 */
public class DeToken
{
    /**
     * Class Members
     */
    // the word itself
    private String m_word = "";
    // the POS tag if any
    private String m_pos = "";
    // the NER tag if any
    private String m_ner = "";
    // the lemma of this word
    private String m_lemma = "";
    // the index of this word in the corresponding sentence
    private int m_index = 0;

    // the synset of this word
    // this member should not be an input but an output.
    // we take the word itself, its POS tag and lemma to 
    // construct a BabelfyToken, then throw the entire
    // sentence where this word is in to Babelfy.
    // Babelfy should return the synset of each word in 
    // the sentence. 
    // each sense will be represented as the sense offset
    // which is a string yet without the prefix 'wn:'.
    private List<String> m_l_synset;

    /**
     * Class Methods
     */
    public DeToken(String word, String pos, String ner, String lemma, int index)
    {
        m_word = word;
        m_pos = pos;
        m_ner = ner;
        m_lemma = lemma;
        m_index = index;
        m_l_synset = null;
    }

    public boolean setSynset(List<String> synset)
    {
        if(synset == null || synset.size() == 0)
        {
            System.out.println("[ERR]: No synset for " + m_word);
            return false;
        }
        m_l_synset = synset;
        return true;
    }

    public String word()
    {
        return m_word;
    }
    
    public String pos()
    {
        return m_pos;
    }

    public String ner()
    {
        return m_ner;
    }

    public String lemma()
    {
        return m_lemma;
    }

    public int index()
    {
        return m_index;
    }
    
    public List<String> synset()
    {
        return m_l_synset;
    }

    public String toTaggedTokenString()
    {
        String synset_str = "";
        if(m_l_synset != null && m_l_synset.size() != 0)
        {
            String.join("+", m_l_synset);
        }
        return String.join("#", m_word, m_pos, m_ner, m_lemma, synset_str);
    }
}
