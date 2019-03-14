package map;

import java.util.HashMap;
import java.util.HashSet;

public class LanguageElect {
    Dictionary dict;

    public LanguageElect(Dictionary dict) {
        this.dict = dict;
    }

    public String getLanguagesWithStats(HashSet<String> words){
        String s = "";
        HashMap<String,Integer> langPoints = findLanguagesWithStats(words);
        for (String lang: langPoints.keySet()) {
            s += lang + ":" + langPoints.get(lang) + "; ";
        }
        return s;
    }

    private HashMap<String,Integer> findLanguagesWithStats(HashSet<String> words){
        HashMap<String,Integer> langPoints = new HashMap<>();
        Language lang;
        for (String word: words) {
            lang = dict.getLanguage(word);
            String lang_tag;

            if(lang != null)
                lang_tag = lang.getTag();
            else
                lang_tag = "##";

            Integer points = langPoints.get(lang_tag);
            if(points != null)
                langPoints.put(lang_tag, points + 1);
            else
                langPoints.put(lang_tag, 1);

        }

        return langPoints;
    }
}
