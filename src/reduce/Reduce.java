package reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * Classe Reducer
 *                                  <Coppia Input><Coppia Output>
 */
public class Reduce extends Reducer<Text, Text, Text, Text> {
    public static String REAL_LANGUAGE_FLAG = "@Real@";

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
        String outputString = "";
        boolean emit = false;
        for (Text inputText : descriptions) {
            inputString = inputText.toString();
            if(inputString.startsWith(REAL_LANGUAGE_FLAG)) {
                outputString = outputString + ("\tReal_Lang:" + inputString.replaceFirst(REAL_LANGUAGE_FLAG, ""));
            }
            else {
                outputString = (inputString) + outputString;
                emit = true;
            }
        }
        if(emit)
            context.write(url, new Text(outputString));
    }
}