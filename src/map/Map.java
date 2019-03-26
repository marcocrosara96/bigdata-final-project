package map;

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
 * Classe Map estende il Mapper e lo implementa
 */
public class Map extends Mapper<LongWritable, Text, Text, Text> {
    public static final String DICTIONARY_FILENAME = "dictionary.json";
    public static final String DICTIONARY_PATH = "/input/dictionary/" + DICTIONARY_FILENAME;
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");//Indent. gli spazi vuoti da word a word
    private static final Pattern WORD_BOUNDARY_V2 = Pattern.compile("[\\[0-9\\]\\s!-$%^&*()_+|~=`{}\\[\\]°:\";'<>?,@#.\\/•]+");
    public static final String WORDS_SPLITTED_TAG = "[WS]";
    private Dictionary dict;
    private LanguageElect langelect;
    private boolean wordsHaveBeenSplitted = false;

    /**
     * Mi permette di fare i settaggi iniziali del mapper tra cui di recuperare ed elaborare tutte le informazioni dei
     * file in Distributed Cache
     * @param context contesto
     */
    @Override
    protected void setup(Context context){
        try{
            //Recupero il file relativo al dizionario dalla cache
            String localPathOfDictionary = null;
            Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            for (Path eachPath : localFiles) {
                if(eachPath.toString().endsWith(DICTIONARY_FILENAME))
                    localPathOfDictionary = eachPath.toString();

            }
            //Carico il dizionario dall'url locale dello stesso
            dict = JsonParser.loadDictionary(localPathOfDictionary);

            //Avvio l'istanza di LanguageElect
            langelect = new LanguageElect(dict);
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

            String valueStringToEmit = langelect.getLanguagesWithStats(getPageWords(lineString));
            if(wordsHaveBeenSplitted == true)
                valueStringToEmit += WORDS_SPLITTED_TAG;
            context.write(new Text(url), new Text(valueStringToEmit));
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
     * @param fullPage pagina completa (inclusa di header)
     * @return url della pagina, estratto dall'header
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
        return filterCleanAddWords(words);
    }

    /**
     * Filtra le parole trovate nel testo rimuvendo quelle non rilevanti e aggiungendone di nuove utili alla rilevazione
     * della lingua alla successiva elaborazione
     * @param words parole da filtrare e integare
     * @return parole filtrate e integrate
     */
    private HashSet<String> filterCleanAddWords(HashSet<String> words){
        wordsHaveBeenSplitted = doWordsStartWithAsianCharacters(words);
        if(wordsHaveBeenSplitted){
            return extendsWordsWithSingleCharcters(words);
        }
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

    /**
     * Analizza un insieme di parole e ritorna true se e solo se almeno una di esse inizia con un carattere orientale
     * @param words insieme di parole
     * @return true se e seolo se una parola inizia con un carattere orientale, false atrimenti
     */
    public boolean doWordsStartWithAsianCharacters(HashSet<String> words){
        boolean asian = false;
        int i = 0;
        for (String word: words) {
            if(word.charAt(i) >= '\u3041') { //Controllo iniziale per evitare di fare inutilmente troppi altri controlli
                if((word.charAt(i) >= '\u3041' && word.charAt(i)<= '\u309F') ||      //Unicode - Hiragana
                        (word.charAt(i) >= '\u30A0' && word.charAt(i)<= '\u30FF') || //Unicode - Katakana
                        (word.charAt(i) >= '\u31F0' && word.charAt(i)<= '\u31FF') || //Unicode - Kat. Phonetic Extensions
                        (word.charAt(i) >= '\u3190' && word.charAt(i)<= '\u319F') || //Unicode - Kanbun
                        (word.charAt(i) >= '\u4E00' && word.charAt(i)<= '\uA000') || //Han Ideographs - Chinese A
                        (word.charAt(i) >= '\u3400' && word.charAt(i)<= '\u4DC0') || //Han Ideographs - Chinese B
                        (word.charAt(i) >= '\uF900' && word.charAt(i)<= '\uFB00') || //Han Ideographs - Chinese C
                        (word.charAt(i) >= '\u9FA6' && word.charAt(i)<= '\u9FCC')    //Han Ideographs - Chinese D
                ) {
                    asian = true;
                    break;
                }
            }
        }
        return asian;
    }

    /**
     * Arricchisce l'insieme di parole con "nuove parole" che sono i singoli caratteri delle parole stesse
     * N.B.[Nell'idea attuale inserisco non solo il carattere singolo ma i caratteri due a due -> per ottimizzare
     * il rilevamento della lingua orientale]
     * @param words insieme di parole da arricchire
     * @return insieme di parole arricchito
     */
    public HashSet<String> extendsWordsWithSingleCharcters(HashSet<String> words){
        HashSet<String> wordsExtended = new HashSet<>();
        for (String word: words) {
            wordsExtended.add(word);
            for(int i = 1; i < word.length(); i++){
                wordsExtended.add(word.substring(i-1,i));
                wordsExtended.add(word.substring(i-1,i+1));
            }
            wordsExtended.add(word.substring(word.length()-1));
        }
        return  wordsExtended;
    }
}
