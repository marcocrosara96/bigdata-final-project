package map;

/**
 * Rappresenta una lingua con un nome e uno/due tag
 */
public class Language {
    private String name;
    private String tag;
    private String tag2;

    public Language(String name, String tag){
        this.name = name;
        this.tag = tag;
    }

    public Language(String name, String tag, String tag2){
        this(name, tag);
        this.tag2 = tag2;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getTag2() {
        return tag2;
    }

    public void setTag2(String tag2) {
        this.tag2 = tag2;
    }

    @Override
    public String toString() {
        String s = "{";
        s += "name:" + name;
        s += ",tag:" + tag;
        if(tag2 != null) s += ",tag2:" + tag2;
        return s + "}";
    }
}
