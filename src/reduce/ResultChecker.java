package reduce;

import java.util.HashSet;

/**
 * Dati i risultati ottenuti e quelli aspettati si occupa di stimare la correttezza e l'accuratezza  degli uni
 * rispetto agli altri, ritornando una stringa con i risultati e un simbolo raffigurante la correttezza
 */
public class ResultChecker {
    public static final char SYMBOL_TICK = '\u2705';
    public static final char SYMBOL_CROSS = '\u274C';
    //public static char SYMBOL_WAVY = '\u3030';
    //public static char SYMBOL_UP = '\u2B06';
    //public static char SYMBOL_DOWN = '\u2B07';
    //public static char SYMBOL_UP_DOWN = '\u2195';
    public static final char SYMBOL_PLUS = '\u2795';
    public static final char SYMBOL_MINUS = '\u2796';
    public static final char SYMBOL_DIVISION = '\u2797';

    /**
     * Ritorna le statistiche sulle lingue trovate e quelle della soluzione
     * Formato: "Response:" + analyzedInfo + " | Expected:" + solutionInfo + "\t" + simbolo_accuratezza
     * @param analyzedInfo lingue trovate tramite l'analisi
     * @param solutionInfo lingue corrispondenti alla soluzione
     * @return stringa nel formato scritto sopra.
     */
    public String getResultsWithAccuracy(String analyzedInfo, String solutionInfo){
        HashSet<String> analyzedResultLangs = arrayToHashSet(analyzedInfo.split(","));
        HashSet<String> solutionResultLangs = arrayToHashSet(solutionInfo.split(","));

        //raffiguro l'accuratezza delle lingue con un simbolo
        char symbol = estimatedAccuracyWithSymbol(analyzedResultLangs, solutionResultLangs);

        //Compongo e ritorno la stringa finale
        return "Response:" + analyzedInfo + " | Expected:" + solutionInfo + "\t" + symbol;
    }

    /**
     * Stima l'accuratezza dei risultati tramite un simbolo di riferimento
     * @param analyzedResultLangs lingue trovate tramite l'analisi
     * @param solutionResultLangs lingue corrispondenti alla soluzione
     * @return simbolo relativo all'accuratezza dei risultati
     */
    private char estimatedAccuracyWithSymbol(HashSet<String> analyzedResultLangs, HashSet<String> solutionResultLangs){
        double percentageOfLess = 0;
        double percentageOfMore = 0;
        //calcolo quante lingue sono presenti nella mia soluzione rispetto a quelle ottenuti dll'analisi
        for (String solution : solutionResultLangs){
            if(analyzedResultLangs.contains(solution))
                percentageOfLess++;
        }
        percentageOfLess = 100 / solutionResultLangs.size() * percentageOfLess;
        //calcolo quante lingue sono presenti nella mia analisi di quelle della soluzione
        for (String result : analyzedResultLangs){
            if(solutionResultLangs.contains(result))
                percentageOfMore++;
        }
        percentageOfMore = 100 / analyzedResultLangs.size() * percentageOfMore;

        if(solutionResultLangs.contains("-"))//se il risultato non mi dice nulla sulla lingua della pagina, assumo il mio ris. corretto
            return SYMBOL_TICK;
        if(percentageOfLess == 100 && percentageOfMore == 100)
            return SYMBOL_TICK;
        if(percentageOfLess == 100 && percentageOfMore < 100)
            return SYMBOL_PLUS;
        if(percentageOfLess < 100 && percentageOfMore == 100)
            return SYMBOL_MINUS;
        if(percentageOfLess == 0 && percentageOfMore == 0)
            return SYMBOL_CROSS;
        return SYMBOL_DIVISION;
    }

    /**
     * Converte un array in HashSet
     * @param array array
     * @return hashset relativo all'array
     */
    private HashSet<String> arrayToHashSet(String[] array){
        HashSet<String> set = new HashSet<>();
        for (String s : array) {
            set.add(s);
        }
        return set;
    }
}
