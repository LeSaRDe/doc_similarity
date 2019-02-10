package docsim;

import java.util.*;
import java.lang.*;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.stream.*;

import edu.stanford.nlp.trees.*;
import edu.stanford.nlp.ling.*;

/**
 * This class is playing a proxy role between CoreNLP and Babel.
 * Each sentence will be annotated, yet not parsed, and stored as
 * an instance of DeSentence, in which each token in this sentence
 * is an instance of DeToken.
 */
public class DeSentence
{
    /**
     * Constants
     */
    /**
     * NOTE:
     * All constituent tags are defined in Penn Treebank II.
     * CoreNLP follows those definitions, and so does NLTK.
     * Try not to customize or self-define any additional tags
     * outside these existing definitions.
     */
    // these tags are the ones in consideration.
    // modifying this list will change the parsing tree pruning.
    private String[] m_constituent_tags =
                        {"ROOT", "S", "SBAR", "SBARQ", "SINV", "SQ",
                        "ADJP", "ADVP", "FRAG", "RRC", "PP", "INTJ",
                        "NP", "NX", "NAC", "NN", "NNS", "NNP", "NNPS", "NP-TMP",
                        "VB", "VBD", "VBG", "VBN", "VBP", "VBZ", "VP",
                        "JJ", "JJR", "JJS", "RB", "RBR", "RBS", "RP"};
    private char[] m_constituent_tags_inits = {'S', 'N', 'A', 'V', 'J', 'R', 'P', 'W', 'C', 'F', 'I', 'L', 'E', 'M', 'U'};

    /**
     * Class Members
     */
    private String m_orig_sentence;
    private ArrayList<DeToken> m_l_tokens;
    private Tree m_constituent_tree;
    private Tree m_tagged_tree;
    private Tree m_tagged_pruned_tree;
    private Tree m_pruned_tree;
    // path for the stopword file
    private String m_swfilepath = Constants.STOPWORDS_PATH;
    // stopword array
    private String[] m_l_stopwords;

    /**
     * Class Methods
     */
    public DeSentence(String orig_sentence)
    {
        m_orig_sentence = orig_sentence;
        m_l_tokens = new ArrayList<DeToken>();
        m_constituent_tree = null;
        m_tagged_tree = null;
        m_pruned_tree = null;
        m_tagged_pruned_tree = null;
        Arrays.sort(m_constituent_tags);
        Arrays.sort(m_constituent_tags_inits);
        m_l_stopwords = loadStopwordList(m_swfilepath);
        Arrays.sort(m_l_stopwords);
    }

    public boolean appendToken(DeToken token)
    {
        if(token == null)
        {
            return false;
        }
        return m_l_tokens.add(token);
    }

    public String getOrigSentence()
    {
        return m_orig_sentence;
    }

    public ArrayList<DeToken> getDeTokens()
    {
        return m_l_tokens;
    }

    public String toTaggedSentenceString()
    {
       return String.join(" ", m_l_tokens.stream().map(detoken->detoken.toTaggedTokenString()).collect(Collectors.toList()));
    }

    public void setConstituentTree(Tree c_tree)
    {
        m_constituent_tree = c_tree;
    }

    public Tree getConstituentTree()
    {
        return m_constituent_tree;
    }

    // this function will attach the sense offsets and pos tags to each leaf.
    // N.B. every leaf node will be formatted as follows: e.g. L:word#12345678n+23456789n#6
    // L:[word]#[synset_1]+...+[synset_n]#[index]
    // if a leaf doesn't have any sense offset, then this leaf is just a token.
    public Tree getTaggedTree()
    {
        // we only want to do this once.
        if(m_tagged_tree == null)
        {
            m_tagged_tree = m_constituent_tree.deepCopy();
        }
        else
        {
            return m_tagged_tree;
        }
        LabelFactory slf = m_tagged_tree.labelFactory();
        List<Tree> leaves = m_tagged_tree.getLeaves();
        for(Tree leaf : leaves)
        {
            DeToken token = m_l_tokens.get(leaves.indexOf(leaf));
            List<String> synset = token.synset();
            int t_idx = token.index();
            String ner = token.ner();
            if(synset != null && synset.size() != 0)
            {
                leaf.setLabel(slf.newLabel("L:" + leaf.value() + "#" + String.join("+", synset) + "#" + t_idx + "#" + ner));
            }
            else
            {
                leaf.setLabel(slf.newLabel("L:" + leaf.value() + "#" + "" + "#" + t_idx + "#" + ner));
            }
        }
        //System.out.println("[DBG]: getTaggedTree: " + m_tagged_tree.toString());
        return m_tagged_tree;
    }

    // this function will take a constituent tree and output a string representation
    // of this tree with each leaf tagged with its sense offset and pos tag.
    // the output string format can be accepted directly by nltk to produce an nltk Tree.
    public StringBuilder printTaggedTree()
    {
        return getTaggedTree().toStringBuilder(new StringBuilder());
    }

    // this function returns a pruned tree.
    // tagged -- if you need a tagged pruned tree
    // (i.e. with all leaves tagged with sense offset and pos).
    public Tree getPrunedTree(boolean tagged)
    {
        if(tagged)
        {
            if(m_tagged_pruned_tree == null)
            {
                m_tagged_pruned_tree = pruneConstituentTree(getTaggedTree());
            }
            //System.out.println("[DBG]: getPrunedTree: " + m_tagged_pruned_tree.toString());
            return m_tagged_pruned_tree;
        }
        else
        {
            if(m_pruned_tree == null)
            {
                m_pruned_tree = pruneConstituentTree(m_constituent_tree);
            }
            return m_pruned_tree;
        }
    }

    // reads stopwords from a file, and outputs a sorted string array
    private String[] loadStopwordList(String swfilepath)
    {
        String [] ret = null;
        try
        {
            File swfile = new File(swfilepath);
            ArrayList<String> swstrbf = new ArrayList<String>();
            BufferedReader bfread = new BufferedReader(new InputStreamReader(new FileInputStream(swfile), "UTF-8"));
            String ln;
            while((ln = bfread.readLine()) != null)
            {
                swstrbf.add(ln);
            }
            ret = swstrbf.toArray(new String[swstrbf.size()]);
        }
        catch(Exception e)
        {
            System.out.println("[ERR]: loadStopwordList " + e.toString());
        }
        Arrays.sort(ret);
        return ret;
    }

    // check if the input word is a stopword
    public boolean isStopword(String word)
    {
        int ret = Arrays.binarySearch(m_l_stopwords, word.toLowerCase());
        //System.out.println("[DBG]: " + "binarySearch " + word + ":" + ret);
        return (ret < 0) ? false : true;
    }

    // check if a constituent tag is in consideration
    public boolean isValidConstituentTag(String tag)
    {
        int ret = Arrays.binarySearch(m_constituent_tags, tag);
        //System.out.println("[DBG]: " + "binarySearch " + word + ":" + ret);
        if (ret < 0)
        {
            ret = Arrays.binarySearch(m_constituent_tags_inits, tag.charAt(0));
            //if(ret < 0)
            //{
            //    System.out.println("[DBG]: tag is not found: " + tag);
            //}
            return (ret < 0) ? false : true;
        }
        return true;
    }
    // check if the input token is valid.
    // since the resulting sentences from the clean-up stage would
    // only contain "-" or "." these two punctuations, then a regluar
    // token should contain "-" at most for "." can be dropped.
    // TODO
    //   sure of course this function can be modified to adapt to
    //   some other scenarios.
    public boolean isValidToken(String token)
    {
        String regex = "[a-zA-Z-.']*#?(\\d{8}[a-z]{1})?(\\+\\d{8}[a-z]{1})*#[0-9]*#[a-zA-Z]*";
        boolean ret = token.matches(regex);
        //if(!ret)
        //{
        //    System.out.println("[DBG]: " + token + " doesn't match!");
        //}
        return ret;
    }

    public boolean isPerson(String token)
    {
        String ner = token.split("#")[3];
        if(ner == null)
        {
            return false;
        }
        return ner.equalsIgnoreCase("PERSON");
    }

    // prune an input tagged constituent tree
    // don't forget that each leaf has been tagged with its sense offset and pos.
    public Tree pruneConstituentTree(Tree c_tree)
    {
        //System.out.println("[DBG]: Enter pruneConstituentTree...");
        //System.out.println("[DBG]:    Tree = " + c_tree.toString());
        // our impl will be a bit flashback here.
        // it means that we don't look at the root of c_tree first,
        // instead, we get all its subtrees pruned, then we take a
        // look at the root.
        // the reason we do it in this way is that, first, the leaves
        // in the tree are considered as Tree's not merely strings, then
        // we can practice our recursion way down to the leaf level, and second,
        // somehow we have to prune every substree regardless of which level
        // we are at here.

        // now check if c_tree is actually just a leaf.
        // if it is, then we check if it is a stopword or an unnecessary token.
        // if the leaf is a stopword or an unnecessary token, then we prune it.
        // otherwise, we return c_tree intact.
        if(c_tree.isLeaf())
        {
            if(isStopword(c_tree.value().substring(2).split("#")[0]) 
                || !isValidToken(c_tree.value().substring(2)) 
                || isPerson(c_tree.value().substring(2)))
            {
                // N.B. at this point, since a Tree cannot remove itself, then
                // we have to return null back its parent, then its parent will
                // remove it.
                //System.out.println("[DBG]: Leaf cut off:" + c_tree.value());
                return null;
            }
            else
            {
                // DBG
                //System.out.println("[DBG]: c_tree value = " + c_tree.value());
                //System.out.println("[DBG]: set c_tree = " + c_tree.value().split("\\|")[0]);
                //c_tree.setValue(c_tree.value().split("\\|")[0]);
                // DBG
                //System.out.println("[DBG]: c_tree = " + c_tree.value());
                return c_tree;
            }
        }
        // if c_tree is not a leaf, then it has at least one child (i.e. a subtree).
        // then we try to prune all of its children subtrees first.
        // after pruning the children, if c_tree still has more than one children (i.e. at least two),
        // then we keep c_tree and its current children, and return c_tree back to its parent if any.
        // on the other hand, if c_tree only has one child left, then we return this child as the pruning
        // result on c_tree (i.e. cascading cut); and if c_tree has no child left,
        // then we return null back to c_tree's parent.
        // N.B. there are two exceptions.
        // 1. if the root of c_tree is 'ROOT', then we keep it anyway.
        // 2. if the root of c_tree is 'SXX', then we don't do cascading cut on it (i.e. 'SXX' can have
        // only one child), but if it has no child left, then we will still cut it off.
        else
        {
            // if the constituent tag of the root of c_tree is not interesting to us
            // (i.e. the entire c_tree is not a phrase in concern)
            // then we directly remove this subtree without doing any further pruning.
            if(!isValidConstituentTag(c_tree.value()))
            {
                return null;
            }

            // otherwise, c_tree is not a leaf, and totally interesting to us,
            // then we look into it to do pruning.
            List<Tree> l_subtrees = c_tree.getChildrenAsList();
            List<Integer> l_rm_subtrees = new ArrayList<Integer>();
            for(int j = 0; j < l_subtrees.size(); j++)
            {
                Tree subtree = l_subtrees.get(j);
                //System.out.println("[DBG]: Try to prune: " + subtree.toString());
                Tree p_subtree = pruneConstituentTree(subtree);
                if(p_subtree == null)
                {
                    //System.out.println("[DBG]: Need to prune this tree: " + j + ":" + subtree.toString());
                    l_rm_subtrees.add(j);
                }
                else
                {
                    c_tree.setChild(j, p_subtree);
                }
            }
            //System.out.println("[DBG]:" + "l_rm_subtrees = " + l_rm_subtrees.toString());
            for(int j = 0; j < l_rm_subtrees.size(); j++)
            {
                // N.B. at this point, the Tree should have been changed.
                //System.out.println("[DBG]: Size of children list is:" + c_tree.getChildrenAsList().size());
                //System.out.println("[DBG]: Remove:" + c_tree.getChild(l_rm_subtrees.get(j)-j).toString());
                c_tree.removeChild(l_rm_subtrees.get(j)-j);
            }
            l_subtrees = c_tree.getChildrenAsList();

            if(c_tree.value().equals("ROOT"))
            {
                return c_tree;
            }
            else if(c_tree.value().matches("^S[A-Z]*")
                && (Arrays.binarySearch(m_constituent_tags, c_tree.value()) != -1))
            {
                if(l_subtrees.size() == 0)
                {
                    // N.B. at this point, we leave 'SXX' to its parent (great chance 'ROOT')
                    // to remove it.
                    return null;
                }
                return c_tree;
            }
            else
            {
                if(l_subtrees.size() == 0)
                {
                    // N.B. at this point, we leave c_tree to its parent to remove it.
                    //System.out.println("[DBG]: To be cut:" + c_tree.toString());
                    return null;
                }
                else if(l_subtrees.size() == 1)
                {
                    // cascading cut
                    //System.out.println("[DBG]: Cascading cut: return: " + l_subtrees.get(0).toString());
                    return l_subtrees.get(0);
                }
                else
                {
                    //System.out.println("[DBG]: Directly return: " + c_tree.toString());
                    return c_tree;
                }
            }
        }
    }
}
