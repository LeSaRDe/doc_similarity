package usersimproj;

import java.util.*;
import java.util.stream.Collectors;

import it.uniroma1.lcl.babelfy.commons.*;
import it.uniroma1.lcl.babelfy.core.*;
import it.uniroma1.lcl.babelfy.commons.annotation.SemanticAnnotation;
import it.uniroma1.lcl.babelnet.*;
import it.uniroma1.lcl.jlt.util.Language;

/**
 * This class is a wrapper for Babelfy. 
 * Basically, we only need this class to provide precise synsets for words
 * in a sentence. So the input should be a sentence (i.e. a DeSentence),
 * and the output synsets should be written back to DeToken of each word.
 */
public class BabelWrap
{
    /**
     * Class Members
     */
    /**
     * TODO
     * private
     */
    public Babelfy m_babelfy;
    public BabelNet m_babelnet;

    /**
     * Class Methods
     */
    public BabelWrap()
    {
        BabelfyParameters bp = new BabelfyParameters();
        bp.setAnnotationResource(BabelfyParameters.SemanticAnnotationResource.WN);
        //bp.setAnnotationResource(BabelfyParameters.SemanticAnnotationResource.BN);
        bp.setMCS(BabelfyParameters.MCS.ON_WITH_STOPWORDS);
        bp.setScoredCandidates(BabelfyParameters.ScoredCandidates.ALL);
        m_babelfy = new Babelfy(bp);
        m_babelnet = BabelNet.getInstance();
    }

    // the synsets will be written back to DeTokens. 
    public boolean getSynsets(DeSentence dsent)
    {
        if(dsent == null)
        {
            System.out.println("[ERR]: Input DeSentence is null!");
            return false;
        }
        boolean ret = true;
        List<BabelfyToken> taggedsent = new ArrayList<BabelfyToken>();
        for(DeToken token : dsent.getDeTokens())
        {
            taggedsent.add(new BabelfyToken(token.word(), token.lemma(), tagToPosTag(token.pos()), Language.EN));
        }
        List<SemanticAnnotation> babelfiedsent = m_babelfy.babelfy(taggedsent, Language.EN);
        //List<SemanticAnnotation> babelfiedsent = m_babelfy.babelfy(dsent.getOrigSentence(), Language.EN);
        for(SemanticAnnotation sa : babelfiedsent)
        {
            List<String> l_wnso = m_babelnet.getSynset(new BabelSynsetID(sa.getBabelSynsetID()))
                                            .getWordNetOffsets()
                                            .stream()
                                            .map(obj->obj.getSimpleOffset())
                                            .collect(Collectors.toList());
            ret = ret & dsent.getDeTokens().get(sa.getTokenOffsetFragment().getStart()).setSynset(new ArrayList<String>(l_wnso));
        }
        return ret;
    }

    private PosTag tagToPosTag (String tag)
    {
        String[] noun = {"NN", "NNS", "NNP", "NNPS"};
        String[] verb = {"VB", "VBD", "VBG", "VBN", "VBP", "VBZ"};
        String[] adv = {"RB", "RBR", "RBS"};
        String[] adj = {"JJ", "JJR", "JJS"};

        if (Arrays.asList(noun).contains(tag))
        {
            return PosTag.NOUN;
        }
        else if(Arrays.asList(verb).contains(tag))
        {
            return PosTag.VERB;
        }
        else if(Arrays.asList(adv).contains(tag))
        {
            return PosTag.ADVERB;
        }
        else if(Arrays.asList(adj).contains(tag))
        {
            return PosTag.ADJECTIVE;
        }
        else
        {
            return PosTag.OTHER;
        }
    }
}
