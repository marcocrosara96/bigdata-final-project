package map;

/**
 * Rappresenta una lingua con un nome e il relativo/i tag nei due formati standard ISO-639-1 e ISO-639-2
 */
public class Language{
    private String name;
    //private String tag_ISO_639_V1; //2 char
    //private String tag_ISO_639_V1_alt; //2 char
    private String tag_ISO_639_V2; //(T) 3 char
    private String tag_ISO_639_V2_alt; //(B) 3 char

    public Language(String name, String tag_ISO_639_V2){
        this.name = name;
        this.tag_ISO_639_V2 = tag_ISO_639_V2;
    }

    public Language(String name, String tag_ISO_639_V2, String tag_ISO_639_V2_alt){
        this(name, tag_ISO_639_V2);
        this.tag_ISO_639_V2_alt = tag_ISO_639_V2_alt;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTag_ISO_639_V2() {
        return tag_ISO_639_V2;
    }

    public void setTag_ISO_639_V2(String tag_ISO_639_V2) {
        this.tag_ISO_639_V2 = tag_ISO_639_V2;
    }

    public String getTag_ISO_639_V2_alt() {
        return tag_ISO_639_V2_alt;
    }

    public void setTag_ISO_639_V2_alt(String tag_ISO_639_V2_alt) {
        this.tag_ISO_639_V2_alt = tag_ISO_639_V2_alt;
    }

    @Override
    public String toString() {
        String s = "{";
        s += "name:" + name;
        s += ",ISO_639_V2:" + tag_ISO_639_V2;
        if(tag_ISO_639_V2_alt != null) s += ",ISO_639_V2_alt:" + tag_ISO_639_V2_alt;
        return s + "}";
    }

    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Language))
            return false;
        Language ll = (Language) o;
        return ((name.equals(ll.name) && tag_ISO_639_V2.equals(ll.tag_ISO_639_V2)));
    }
}
