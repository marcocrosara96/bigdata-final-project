package map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private long numRecords = 0;
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    /**
     *
     * @param offset [non lo usiamo]
     * @param lineText --> pagina con il relativo header che ci arriva da NLinesRecordReader
     * @param context --> collegamento al reducer
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            /*String line = lineText.toString();
            Text currentWord = new Text(); //facciamo così per evitare di ricrearlo ad ogni ciclo
            for (String word : WORD_BOUNDARY.split(line)) {
                if (word.isEmpty()) {
                    continue;
                }
                currentWord.set(word); //NB <--- Set
                context.write(currentWord, one); //one --> IntWritable (vedi sopra)
            }*/

        //System.out.println("map-line: ");
        context.write(lineText, new IntWritable(1)); //one --> IntWritable (vedi sopra)
    }

    /**
     * Mi permette di recuperare tutti i path dei file in Distributed Cache
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    /*@Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try{
            Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if(stopWordsFiles != null && stopWordsFiles.length > 0) {
                for(Path stopWordFile : stopWordsFiles) {
                    //readFile(stopWordFile);
                }
            }
        } catch(IOException ex) {
            System.err.println("Exception in mapper setup: " + ex.getMessage());
        }
    }*/


}
