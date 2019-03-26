package reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Classe Reduce estende il Reducer e lo implementa
 *                                  <Coppia Input><Coppia Output>
 */
public class Reduce extends Reducer<Text, Text, Text, Text> {
    public static final String REAL_LANGUAGE_FLAG = "@Real@";
    private ResultChecker resultchecker = new ResultChecker();

    /**
     * !!! REDUCER !!!
     * @param url url della pagina
     * @param descriptions informazione strutturata sulla lingua della pagina a cui appartiene l'url
     * @param context contesto
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
            String outputString = resultchecker.getResultsWithAccuracy(analyzedInfoSlitted[0], solutionInfo) + "\t" + analyzedInfoSlitted[1];
            context.write(url, new Text(outputString));
        }
    }
}