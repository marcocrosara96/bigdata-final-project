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

    public static Language NOTFOUND_WORDS_LANG = new Language("Not_Found","XX");
    public static Language OTHER_WORDS_LANG = new Language("Other_Lang","++");
    public static String LANGUAGE_NOT_FOUND = "-";
    public static String SEPARATOR = ";";
    //Settaggi sulla lingua rilevata
    private static int MAX_NUMBER_LANG_TO_SHOW = 6; //Settaggio che dice quante lingue mostrare nelle statistiche(le più probabili)
    private static double VALID_LANGUAGE_THRESHOLD = 1.0; //percentuale sopra il quale una linqua e valida

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
        String s = "";
        HashMap<Language,Double> langStats = fromPointsToStats(findLanguagesWithPoints(words), words.size());

        for (Language lang: langStats.keySet()) {
            if(!lang.equals(NOTFOUND_WORDS_LANG) && !lang.equals(OTHER_WORDS_LANG))
                s += lang.getTag_ISO_639_V2() + ":" + langStats.get(lang) + "%" + SEPARATOR;
        }

        //Aggiungo anche Other_Langs (se esiste) e Not_Found
        if(langStats.containsKey(OTHER_WORDS_LANG))
            s += "Other_Langs:" + langStats.get(OTHER_WORDS_LANG) + "%" + SEPARATOR;
        s += "Not_Found:" + langStats.get(NOTFOUND_WORDS_LANG) + "%";
        return calculateObtainedResult(langStats) + "\t" + s;
    }

    /**
     * Conta le parole che appartengono ad ogni lingua
     * @param words parole di cui cercare la lingua
     * @return associazioni tra lingua e numero parole trovate
     */
    private HashMap<Language,Integer> findLanguagesWithPoints(HashSet<String> words){
        HashMap<Language,Integer> langPoints = new HashMap<>();
        Language lang;
        for (String word: words) {
            lang = dict.getLanguage(word);

            if(lang == null)
                lang = NOTFOUND_WORDS_LANG;

            Integer points = langPoints.get(lang);
            if(points != null)
                langPoints.put(lang, points + 1);
            else
                langPoints.put(lang, 1);
        }

        return langPoints;
    }

    /**
     * Trasforma il numero di parole trovate per ogni lingua in una percentuale di appartenenza del testo alle lingue
     * @param langPoints numero di parole trovate per ogni lingua
     * @param totWords totale delle parole del testo
     * @return associazioni tra lingua e statistica percentuale di appartenenza del testo alla lingua
     */
    private HashMap<Language, Double> fromPointsToStats(HashMap<Language,Integer> langPoints, int totWords){
        //calcolo delle percentuali
        HashMap<Language,Double> langStats = new HashMap<>();
        for(Language new_lang: langPoints.keySet()){
            //--------------------------------------------------
            //scelgo se inserire o meno la nuova lingua nelle statistiche (inserisco al massimo N_LANG_TO_SHOW lingue,
            //quelle con la percentuale più elevata), le parole non trovate ## sono inserite a parte
            //esempio con N_LANG_TO_SHOW = 2
            //    LINGUA_1 : 21% | LINGUA_2 : 12% | ALTRE_LINGUE = 67% | NON_TROVATE = 4%
            double percentage = roundsUp(100 / (totWords * 1.0) * langPoints.get(new_lang), 2);
                                                                //1.0 <-- altrimenti il risultato è un integer
            if(langStats.size() < MAX_NUMBER_LANG_TO_SHOW)//se c'è posto lo inserisco subito
                langStats.put(new_lang, percentage);
            else{//se non c'è posto cerco la presenza di un candidato con meno percentuale
                Pair<Language, Double> candidate = minValueKey(langStats);
                if(percentage > candidate.getValue()){
                    langStats.remove(candidate.getKey());
                    langStats.put(new_lang, percentage);
                }
            }
            //--------------------------------------------------
        }
        langStats.put(OTHER_WORDS_LANG, calculateRemaining(langStats));
        return langStats;
    }

    private Pair<Language, Double> minValueKey(HashMap<Language,Double> langStats){
        double min = 101;
        Language langMin = null;
        for(Language lang_in : langStats.keySet()) {
            if(langStats.get(lang_in) < min){
                min = langStats.get(lang_in);
                langMin = lang_in;
            }
        }
        return new Pair<>(langMin, min);
    }

    private Pair<Language, Double> maxValueKey(HashMap<Language,Double> langStats){
        double max = -1;
        Language langMax = null;
        for(Language lang_in : langStats.keySet()) {
            if(langStats.get(lang_in) > max){
                max = langStats.get(lang_in);
                langMax = lang_in;
            }
        }
        return new Pair<>(langMax, max);
    }

    private double calculateRemaining(HashMap<Language,Double> langStats){
        double sum = 0;
        for (Language lang_in : langStats.keySet()) {
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

    /**
     * Calcolo le lingue in cui secondo l'algoritmo è scritta la pagina, basandomi sul VALID_LANGUAGE_THRESHOLD e
     * MAX_NUMBER_LANG_TO_SHOW
     * @param langStats lingue con le relative percentuali di presenza
     * @return lista delle lingue della pagina
     */
    public String calculateObtainedResult(HashMap<Language, Double> langStats){
        HashMap<Language, Double> langsToReturn_inList = new HashMap<>();
        Language alternative = null;
        int i = 0;
        for (Language lang: langStats.keySet()) { //seleziono le prime MAX_NUMBER_LANG_TO_SHOW lingue che verificano il limite minimo
            if(!lang.equals(NOTFOUND_WORDS_LANG) && !lang.equals(OTHER_WORDS_LANG) && i < MAX_NUMBER_LANG_TO_SHOW){
                if(langStats.get(lang) >= VALID_LANGUAGE_THRESHOLD){
                    langsToReturn_inList.put(lang, langStats.get(lang));
                    i++;
                }
                else{ // salvo la lingua con la percentuale massima
                    if(alternative == null || langStats.get(lang) > langStats.get(alternative))
                        alternative = lang;
                }
            }
        }
        if(langsToReturn_inList.size() != 0)
            return fromLangsToString(clearIfBigStep(langsToReturn_inList));
        if(alternative != null) //se non ho selezionato nessuna lingua sopra il threshold, scelgo quella che ha comunque la percentuale massima
            return alternative.getTag_ISO_639_V2();
        return LANGUAGE_NOT_FOUND;
    }

    public HashMap<Language, Double> clearIfBigStep(HashMap<Language, Double> langStats){
        Pair<Language, Double> langMax = maxValueKey(langStats);
        if(langMax.getValue() == null)
            return langStats;
        if(langMax.getValue() < VALID_LANGUAGE_THRESHOLD)
            return langStats;
        if(langMax.getValue() > 10)
            langMax = new Pair<>(langMax.getKey(), 10.01); //Attenzione non sto modificando il vlore reale
        double step = langMax.getValue() - 3;

        HashMap<Language, Double> newLangStats = new HashMap<>();
        for (Language lang: langStats.keySet()) {
            if(langStats.get(lang) > step){
                newLangStats.put(lang, langStats.get(lang));
            }
        }
        return newLangStats;
    }

    public String fromLangsToString(HashSet<Language> langs){
        String s = "";
        for (Language lang: langs) {
            s += lang.getTag_ISO_639_V2() + ",";
        }
        if(!s.equals(""))
            return s.substring(0, s.length()-1); //tolgo la virgola in eccesso
        return s;
    }

    public String fromLangsToString(HashMap<Language, Double> langsAndStats){
        HashSet<Language> langs = new HashSet<>(langsAndStats.keySet());
        return fromLangsToString(langs);
    }
}
