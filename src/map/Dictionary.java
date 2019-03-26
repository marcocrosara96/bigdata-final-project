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

    /**
     * Data una parola ritorna la lingua a cui è associata
     * @param word parola di cui cercare la lingua
     * @return lingua della parola
     */
    public Language getLanguage(String word){
        return dict.get(word);
    }

    /*public Language getLanguageAdvanced(String word){
        Language l = dict.get(word);
        if(l != null)
            return dict.get(word);
        for(String dictWorld : dict.keySet()){
            if(dictWorld.contains(word))
                return dict.get(dictWorld);
        }
        return null;
    }*/

    @Override
    public String toString(){
        String s = "info:" + dictName + "-" + dictVersion + "-" + dictType + ":";
        for (String key : dict.keySet()) {
            s += "{word:" + key + ",language:" + dict.get(key) + "}\n";
        }
        return s;
    }
}
