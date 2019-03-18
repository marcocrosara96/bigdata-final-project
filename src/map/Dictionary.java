package map;

import java.util.HashMap;

/**
 * Rappresenta un dizionario, ovvero un elenco di parole a ognuna delle quali è associata una specifica lingua
 */
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

    public void addWord(String word, Language language){
        dict.put(word, language);
    }

    public Language getLanguage(String word){
        return dict.get(word);
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
