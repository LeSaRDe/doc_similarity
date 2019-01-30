package usersimproj;

/**
 * This class is the data structure of a user text record.
 * The fields are consistent with the schema of the user text db.
 */
class UserTextRec
{
    private String m_user_id;
    private String m_time;
    private String m_clean_text;
    private String m_tagged_text;
    private String m_parse_trees;

    public UserTextRec(String user_id, String time, String clean_text,
                        String tagged_text, String parse_trees)
    {
        if(user_id == null || time == null || clean_text == null)
        {
            System.out.println("[ERR]: UserTextRec cannot be constructed!");
            return;
        }
        m_user_id = new String(user_id);
        m_time = new String(time);
        m_clean_text = new String(clean_text);
        m_tagged_text = (tagged_text == null) ? null : new String(tagged_text);
        m_parse_trees = (parse_trees == null) ? null : new String(parse_trees);
    }

    public String getuserid()
    {
        return m_user_id;
    }

    public String gettime()
    {
        return m_time;
    }

    public String getcleantext()
    {
        return m_clean_text;
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
