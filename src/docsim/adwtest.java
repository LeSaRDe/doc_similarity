package docsim;

import it.uniroma1.lcl.adw.*;
import it.uniroma1.lcl.adw.comparison.*;
import it.uniroma1.lcl.adw.comparison.WeightedOverlap;
import java.lang.*;


class adwtest
{
    static public void main(String[] argv)
    {
        ADW pipeline = new ADW();
        String w1 = "like";
        String w2 = "love";
        ItemType w1_type = ItemType.SURFACE;
        ItemType w2_type = ItemType.SURFACE;
        DisambiguationMethod disMethod = DisambiguationMethod.NONE;
        SignatureComparison measure = new WeightedOverlap();
        double score1 = pipeline.getPairSimilarity(
                w1, w2,
                disMethod,
                measure,
                w1_type, w2_type);
        System.out.println("score = " + Double.toString(score1)); 
    }
}
