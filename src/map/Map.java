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
    public static String DICTIONARY_100 = "/input/dictionary_100.json";
    private Dictionary dict;
    //private final static IntWritable one = new IntWritable(1);
    //private Text word = new Text();
    //private long numRecords = 0;
    //private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    /**
     * Mi permette di fare i settaggi iniziali del mapper tra cui di recuperare ed elaborare tutte le informazioni dei
     * file in Distributed Cache
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try{
            dict = JsonParser.loadDictionary100(DICTIONARY_100);
        } catch(Exception e) {
            System.err.println("Exception in mapper setup: " + e.getMessage());
        }
    }

    /**
     *
     * @param offset [non lo usiamo]
     * @param lineText --> pagina con il relativo header che ci arriva da NLinesRecordReader
     * @param context --> collegamento al reducer
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
        String url = getPageUrlFromHeader(lineText.toString());

        if(url == null)
            return;

        context.write(new Text(url), new IntWritable(1)); //one --> IntWritable (vedi sopra)



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
    }

    /**
     * Return Url from Header of Page
     * EXAMPLE:
     * WARC-Type: conversion
     * WARC-Target-URI: http://url.com
     * WARC-Date: 2019-02-15T19:26:02Z
     * WARC-Record-ID: <urn:uuid:a9b6429d-50cc-4ebd-b259-9883acf7db08>
     * WARC-Refers-To: <urn:uuid:d882c230-4df2-436b-ba66-4cfec0d49c4e>
     * WARC-Block-Digest: sha1:CUQQM3WGK2FKF4QQM5YXWJ5PTXS4NZOU
     * Content-Type: text/plain
     * Content-Length: 4691
     * @param fullPage
     * @return
     */
    private String getPageUrlFromHeader(String fullPage){
        int urlStartIndex = fullPage.indexOf("WARC-Target-URI: ");
        if(urlStartIndex == -1)
            return null;
        else {
            int urlEndIndex = fullPage.indexOf("WARC-Date: ") - 1;
            return fullPage.substring(urlStartIndex + 17, urlEndIndex);
        }
    }

    /**
     *
     * @param fullPage
     * @return
     */
    private String getPageText(String fullPage){
        String textPage = null;
        return textPage;
    }
}
