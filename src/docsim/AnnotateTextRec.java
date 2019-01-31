package docsim;

/**
 * This class is the data structure of a user text record.
 * The fields are consistent with the schema of the user text db.
 */
class AnnotateTextRec
{
    private String m_docid;
    private String m_prener_text;
    private String m_tagged_text;
    private String m_parse_trees;

    public AnnotateTextRec(String docid, String prener_text, String tagged_text, String parse_trees)
    {
        if(docid == null || prener_text == null)
        {
            System.out.println("[ERR]: AnnotateTextRec cannot be constructed!");
            return;
        }
        m_docid = new String(docid);
        m_prener_text = new String(prener_text);
        m_tagged_text = (tagged_text == null) ? null : new String(tagged_text);
        m_parse_trees = (parse_trees == null) ? null : new String(parse_trees);
    }

    public String getdocid()
    {
        return m_docid;
    }

    public String getprenertext()
    {
        return m_prener_text;
    }

    public String gettaggedtext()
    {
        return m_tagged_text;
    }

    public String getparsetrees()
    {
        return m_parse_trees;
    }

    public void settaggedtext(String tagged_text)
    {
        m_tagged_text = new String(tagged_text);
    }

    public void setparsetrees(String parse_trees)
    {
        m_parse_trees = new String(parse_trees);
    }
}
