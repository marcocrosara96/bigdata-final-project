package reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

/**
 * Classe Reducer
 *                                  <Coppia Input><Coppia Output>
 */
public class Reduce extends Reducer<Text, Text, Text, Text> {
    public static String REAL_LANGUAGE_FLAG = "@Real@";
    public static char SYMBOL_TICK = '\u2705';
    public static char SYMBOL_CROSS = '\u274C';
    public static char SYMBOL_WAVY = '\u3030';

    /**
     * !!! REDUCER !!!
     * @param url url della pagina
     * @param descriptions informazione strutturata sulla lingua della pagina a cui appartiene l'url
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text url, Iterable<Text> descriptions, Context context) throws IOException, InterruptedException {
        String inputString;
        String anlyzedInfo = "";
        String solutionInfo = "";
        boolean emit = false;
        for (Text inputText : descriptions) {
            inputString = inputText.toString();
            if(inputString.startsWith(REAL_LANGUAGE_FLAG)) {
                solutionInfo = inputString.replaceFirst(REAL_LANGUAGE_FLAG, "");
            }
            else {
                anlyzedInfo = inputString;
                emit = true;
            }
        }
        if(emit) {
            String[] analyzedInfoSlitted = anlyzedInfo.split("\t");
            String outputString = analyzeResults(analyzedInfoSlitted[0], solutionInfo) + "\t" + analyzedInfoSlitted[1];
            context.write(url, new Text(outputString));
        }
    }

    public String analyzeResults(String analyzedInfo, String solutionInfo){
        HashSet<String> analyzedResultLangs = arrayToHashSet(analyzedInfo.split(","));
        HashSet<String> solutionResultLangs = arrayToHashSet(solutionInfo.split(","));
        double percentage = 0;
        for (String solution : solutionResultLangs){
            if(analyzedResultLangs.contains(solution))
                percentage++;
        }
        percentage = 100 / solutionResultLangs.size() * percentage;

        //Compongo la stringa finale
        String s = "Response:" + analyzedInfo + " - Expected:" + solutionInfo + "\t";
        s += (percentage > 50) ? SYMBOL_TICK : ((percentage > 30) ? SYMBOL_WAVY : SYMBOL_CROSS);
        return s;
    }

    public HashSet<String> arrayToHashSet(String[] array){
        HashSet<String> set = new HashSet<>();
        for (String s : array) {
            set.add(s);
        }
        return set;
    }
}