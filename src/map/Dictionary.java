package map;

import java.util.HashMap;

public class Dictionary {
    private String dictName;
    private String dictType;
    private String dictVersion;

    private HashMap<String, Language> dict;

    public Dictionary(String dictName, String dictType, String dictVersion) {
        this.dictName = dictName;
        this.dictType = dictType;
        this.dictVersion = dictVersion;
        dict = new HashMap<>();
    }

    public void addWord(String name, Language language){
        dict.put(name, language);
    }

    @Override
    public String toString(){
        String s = "";
        for (String key : dict.keySet()) {
            s += "{word:" + key + ",language:" + dict.get(key) + "}\n";
        }
        return s;
    }
}
