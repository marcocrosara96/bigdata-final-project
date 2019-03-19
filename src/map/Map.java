package map;

import inputFormat.PageAndHeaderInputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import reduce.Reduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.regex.Pattern;

/**
 * Classe Mapper
 */
public class Map extends Mapper<LongWritable, Text, Text, Text> {
    public static String DICTIONARY_FILENAME = "dictionary.json";
    public static String DICTIONARY_PATH = "/input/dictionary/" + DICTIONARY_FILENAME;
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");//Indent. gli spazi vuoti da word a word
    private static final Pattern WORD_BOUNDARY_V2 = Pattern.compile("[\\[0-9\\]\\s!-$%^&*()_+|~=`{}\\[\\]°:\";'<>?,@#.\\/•]+");
    private Dictionary dict;
    private LanguageElect langelect;
    private HashSet<String> wordsExcluded;

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
            //Recupero il file relativo al dizionario dalla cache
            String localPathOfDictionary100 = null;
            Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            for (Path eachPath : localFiles) {
                if(eachPath.toString().endsWith(DICTIONARY_FILENAME))
                    localPathOfDictionary100 = eachPath.toString();

            }
            //Carico il dizionario dall'url locale dello stesso
            dict = JsonParser.loadDictionary(localPathOfDictionary100);

            //Avvio l'istanza di LanguageElect
            langelect = new LanguageElect(dict);

            //Setto le parole da escludere
            wordsExcluded = new HashSet<>();
            wordsExcluded.add("http");
            wordsExcluded.add("https");
        } catch(Exception e) {
            System.err.println("Exception in mapper setup: " + e.getMessage());
        }
    }

    /**
     * !!! MAPPER !!!
     * @param offset [non lo usiamo]
     * @param lineText --> pagina con il relativo header che ci arriva da NLinesRecordReader
     * @param context --> collegamento al reducer
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
        //Cerco di capire se lineText arriva da un file WET o da un file INFO, verrà processato in modo diverso
        String lineString = lineText.toString();
        if(!lineString.startsWith("http")) {//PROVENIENZA INPUT: FILE WET
            String url = getPageUrlFromHeader(lineString);
            if(url == null)
                return;

            //context.write(new Text(url), wordsToText(getPageWords(lineText.toString()))); //one --> IntWritable (vedi sopra)
            context.write(new Text(url), new Text(
                    langelect.getLanguagesWithStats(
                            getPageWords(lineString))));
        }
        else{//PROVENIENZA INPUT: FILE INPUT
            String[] tuple = lineString.split("\t"); // formato: URL \t LANG \t CHARSET
            context.write(new Text(tuple[0]), new Text(Reduce.REAL_LANGUAGE_FLAG  + tuple[1]));
        }

        //NB <--- use Set per assegnare la stringa al testo !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!11!!!!
    }

    /**
     * Ritorna l'url di una pagina, estraendolo dal suo header
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
     * Ritorna il testo della pagina privo dell'header
     * @param fullPage = header + testo pagina
     * @return testo della pagina(senza header)
     */
    private String getPageText(String fullPage){
        int lastLineIndex = fullPage.indexOf("Content-Length:");
        int cutIndex = fullPage.indexOf('\n',lastLineIndex);
        return fullPage.substring(cutIndex + 1);
    }

    /**
     * Ritorna la lista delle parole nel testo, applica i filtri necessari
     * @param fullPage = header + testo pagina
     * @return le parole trovate nel testo della pagina
     */
    private HashSet<String> getPageWords(String fullPage){
        String textPage = getPageText(fullPage);
        HashSet<String> words = new HashSet<>();
        for (String word : WORD_BOUNDARY_V2.split(textPage)) {
            if (word.isEmpty())
                continue;
            words.add(word);
        }
        return filterAndCleanWords(words);
    }

    /**
     * Filtra le parole trovate nel testo rimuvendo quelle non rilevanti
     * @param words parole da filtrare
     * @return parole filtrate
     */
    private HashSet<String> filterAndCleanWords(HashSet<String> words){
        /*HashSet<String> wordsFiltered = new HashSet<>();
        for (String word: words) {
            if(!wordsExcluded.contains(word))
                wordsFiltered.add(word);
        }
        return wordsFiltered;*/
        return words;
    }

    /**
     * Genera una stringa [Text] che visualizza in sequenza un insieme di parole
     * @param words insieme di parole
     * @return Text : stringa con la seguenza di parole
     */
    private Text wordsToText(HashSet<String> words){
        String output = "";
        for (String word: words) {
            output += word + ";";
        }
        return new Text(output);
    }
}
