package map;

import org.apache.commons.math3.util.Pair;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Si occupa di "Promuovere" ovvero decidere la lingua di un testo, dato in ingresso il relativo insieme di parole
 */
public class LanguageElect {
    Dictionary dict;

    public static String NOTFOUND_LANG_WORDS_TAG = "##";
    public static String OTHER_LANG_WORDS_TAG = "++";
    public static String SEPARATOR = " | ";
    private static int N_LANG_TO_SHOW = 5; //Settaggio che dice quante lingue mostrare nelle statistiche(le più probabili)

    public LanguageElect(Dictionary dict) {
        this.dict = dict;
    }

    /**
     * Ritorna una stringa con la lista di lingue riconosciute nel testo e le relative statistiche sulla lingua più
     * probabile
     * @param words parole del testo su cui viene fatta la ricerca della lingua
     * @return stringa con le statistiche
     */
    public String getLanguagesWithStats(HashSet<String> words){
        String s = "| ";
        HashMap<String,Integer> langPoints = findLanguagesWithPoints(words);
        HashMap<String,Double> langStats = fromPointsToStats(langPoints, words.size());

        for (String lang: langStats.keySet()) {
            if(!lang.equals(OTHER_LANG_WORDS_TAG ) && !lang.equals(NOTFOUND_LANG_WORDS_TAG))
            s += lang + ":" + langStats.get(lang) + SEPARATOR;
        }
        s += "Other_Langs:" + langStats.get(OTHER_LANG_WORDS_TAG) + SEPARATOR;
        if(langStats.containsKey(NOTFOUND_LANG_WORDS_TAG))
            s += "Not_Found:" + langStats.get(NOTFOUND_LANG_WORDS_TAG) + SEPARATOR;
        return s;
    }

    /**
     * Conta le parole che appartengono ad ogni lingua
     * @param words parole di cui cercare la lingua
     * @return associazioni tra lingua e numero parole trovate
     */
    private HashMap<String,Integer> findLanguagesWithPoints(HashSet<String> words){
        HashMap<String,Integer> langPoints = new HashMap<>();
        Language lang;
        for (String word: words) {
            lang = dict.getLanguage(word);
            String lang_tag;

            if(lang != null)
                lang_tag = lang.getTag();
            else
                lang_tag = NOTFOUND_LANG_WORDS_TAG;

            Integer points = langPoints.get(lang_tag);
            if(points != null)
                langPoints.put(lang_tag, points + 1);
            else
                langPoints.put(lang_tag, 1);
        }

        return langPoints;
    }

    /**
     * Trasforma il numero di parole trovate per ogni lingua in una percentuale di appartenenza del testo alle lingue
     * @param langPoints numero di parole trovate per ogni lingua
     * @param totWords totale delle parole del testo
     * @return associazioni tra lingua e statistica percentuale di appartenenza del testo alla lingua
     */
    private HashMap<String, Double> fromPointsToStats(HashMap<String,Integer> langPoints, int totWords){
        //calcolo delle percentuali
        HashMap<String,Double> langStats = new HashMap<>();
        for(String new_lang: langPoints.keySet()){
            //--------------------------------------------------
            //scelgo se inserire o meno la nuova lingua nelle statistiche (inserisco al massimo N_LANG_TO_SHOW lingue,
            //quelle con la percentuale più elevata), le parole non trovate ## sono inserite a parte
            //esempio con N_LANG_TO_SHOW = 2
            //    LINGUA_1 : 21% | LINGUA_2 : 12% | ALTRE_LINGUE = 67% | NON_TROVATE = 4%
            double percentage = roundsUp(100 / (totWords * 1.0) * langPoints.get(new_lang), 2);
                                                                //1.0 <-- altrimenti il risultato è un integer
            if(langStats.size() < N_LANG_TO_SHOW)//se c'è posto lo inserisco subito
                langStats.put(new_lang, percentage);
            else{//se non c'è posto cerco la presenza di un candidato con meno percentuale
                Pair<String, Double> candidate = minValueKey(langStats);
                if(percentage > candidate.getValue()){
                    langStats.remove(candidate.getKey());
                    langStats.put(new_lang, percentage);
                }
            }
            //--------------------------------------------------
        }
        langStats.put(OTHER_LANG_WORDS_TAG, calculateRemaining(langStats));
        return langStats;
    }

    private Pair<String, Double> minValueKey(HashMap<String,Double> langStats){
        double min = 101;
        String langMin = null;
        for(String lang_in : langStats.keySet()) {
            if(langStats.get(lang_in) < min){
                min = langStats.get(lang_in);
                langMin = lang_in;
            }
        }
        return new Pair<>(langMin, min);
    }

    private double calculateRemaining(HashMap<String,Double> langStats){
        double sum = 0;
        for (String lang_in : langStats.keySet()) {
            sum += langStats.get(lang_in);
        }
        return ((100-sum > 0) ? roundsUp(100-sum, 2) : 0); //se il rimanente è minore di zero allora ritorna 0
    }

    /**
     * Svolge un arrotondamento per Eccesso
     * @param value valore da arrotondare
     * @param numDecPlaces numero di cifre dopo la virgola a cui arrotondare
     * @return il valore arrotondato per eccesso
     */
    public static double roundsUp(double value, int numDecPlaces) {
        BigDecimal bd = new BigDecimal(value).setScale(numDecPlaces, BigDecimal.ROUND_HALF_EVEN/*, BigDecimal.ROUND_UP*/);
        return bd.doubleValue();
    }
}
