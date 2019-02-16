package  docsim;

import java.util.*;
import java.util.regex.*;

import it.uniroma1.lcl.adw.*;
import it.uniroma1.lcl.adw.comparison.*;

class ADWWrap
{
    /**
     * Class Memebers
     */
    private ADW m_adw;
    private SignatureComparison m_measure;

    /**
     * Class Methods
     */
    public ADWWrap()
    {
        m_adw = new ADW();
        m_measure = new WeightedOverlap();
    }

    private boolean check_word_pos(String word_pos)
    {
        if(word_pos == null)
        {
            return false;
        }
        String pattern = "[a-zA-Z.-']*#[a-zA-Z.-']*";
        //Pattern p = Pattern.compile(pattern);
        //Matcher m = p.matcher(word_pos);
        //boolean ret = m.matches();
        boolean ret = word_pos.matches(pattern);
        //boolean ret = Pattern.matches(pattern, word_pos);
        if(!ret)
            System.out.println("[DBG]: Input word: " + word_pos + " is invalid!");
        return ret; 
    }

    public Double getWordPairSimilarity(String word_pos_1, String word_pos_2)
    {
        Double ret = 0D;
        //if(check_word_pos(word_pos_1) && check_word_pos(word_pos_2))
        {
            ret = m_adw.getPairSimilarity(word_pos_1, word_pos_2, 
                    DisambiguationMethod.ALIGNMENT_BASED, m_measure, ItemType.SURFACE_TAGGED, ItemType.SURFACE_TAGGED);
            //ret = m_adw.getPairSimilarity(word_pos_1, word_pos_2, 
            //        DisambiguationMethod.NONE, m_measure, ItemType.SURFACE_TAGGED, ItemType.SURFACE_TAGGED);
            //System.out.println("[DBG]: sim = " + ret.toString());
        }
        //System.out.println("[DBG]: get sim words = " + word_pos_1 + " " + word_pos_2);
        return ret;
    }

    // compare all possible synset pairs corresponding to the two words,
    // and return the best score.
    // inputs are WordNet offset lists, of the format: e.g. 12345678n
    // the first 8 digits are the offset and the last letter is the POS tag.
    // ADW requires the input to be of the format like this: 12345678-n, with a '-' in between.
    public Double getWordPairSimilarity(List<String> l_offset_1, List<String> l_offset_2)
    {
        //System.out.println("[DBG]: l_offset_1 len = " + l_offset_1.size() + ":" + "l_offset_2 len = " + l_offset_2.size() + ":" + "l_offset_1 = " + l_offset_1.toString() + ":" + "l_offset_2 = " + l_offset_2.toString());
        Double ret = 0D;
        if(l_offset_1 == null || l_offset_2 == null || l_offset_1.size() == 0 || l_offset_2.size() == 0)
        {
            return ret;
        }

        //these two local variables are only for testing
        String f_os_1 = null;
        String f_os_2 = null;

        for(String offset_1 : l_offset_1)
        {
            for(String offset_2 : l_offset_2)
            {
                if(offset_1.equals("") || offset_2.equals(""))
                {
                    return ret;
                }
                //System.out.println("[DBG]: offset_1 = " + offset_1 + ":" + "offset_2 = " + offset_2);
                Double cur_score = m_adw.getPairSimilarity(
                    offset_1.substring(0, 8) + "-" + offset_1.substring(8),
                    offset_2.substring(0, 8) + "-" + offset_2.substring(8),
                    DisambiguationMethod.ALIGNMENT_BASED,
                    m_measure,
                    ItemType.SENSE_OFFSETS,
                    ItemType.SENSE_OFFSETS);
                if(cur_score > ret)
                {
                    f_os_1 = new String(offset_1);
                    f_os_2 = new String(offset_2);

                    ret = cur_score;
                }
            }
        }
        if(ret >= 1D)
        {
            ret = 1D;
        }
        //only for testing
        System.out.println("[DBG]:" + " ADW: " + "offset_1 = " + f_os_1 + ", offet_2 = " + f_os_2);
        return ret;
    }
}
