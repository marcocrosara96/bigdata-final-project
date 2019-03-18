package map;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Parser per i file json dei DIZIONARI
 */
public class JsonParser {
    public static String CHARSET = "UTF-8";

    /**
     * Parserizza il testo del dizionario per ricavarne le info sulle lingue e sulle relative parole
     * @param filePath path del dizionario sulla  distributed cache
     * @return il dizionario popolato correttamente con lingue e relative parole
     */
    public static Dictionary loadDictionary(String filePath){
        Dictionary dict = null;
        try {
            String jsonDoc = readJSON(filePath);
            JSONObject obj = new JSONObject(jsonDoc);
            dict = new Dictionary(obj.getString("name"),
                    obj.getString("type"),
                    obj.getString("version"));

            //Itero sui LINGUAGGI del dizionario
            JSONArray languagesArr = obj.getJSONArray("languages");
            for (int i = 0; i < languagesArr.length(); i++){
                JSONObject language = languagesArr.getJSONObject(i);
                Language l = new Language(language.getString("name"),
                                        language.getString("tag"));
                //tag2 -> opzionale
                if(language.has("tag2"))
                    l.setTag2(language.getString("tag2"));

                //Itero sulle PAROLE del linguaggio
                JSONArray wordsArr = language.getJSONArray("words");
                for (int j = 0; j < wordsArr.length(); j++){
                    dict.addWord(wordsArr.getString(j), l);
                }
            }
        }catch (Exception e){
            System.err.println("Exception while PARSING file " + filePath + " : " + e.getMessage());
        }
        return dict;
    }

    /**
     * Estrae il testo del file json:dizionario presente sulla distributed cache
     * @param filePath path del dizionario sulla  distributed cache
     * @return testo del dizionario
     */
    public static String readJSON(String filePath){
        String s = null;
        try{
            File file = new File(filePath);
            FileInputStream fis = new FileInputStream(file);
            byte[] data = new byte[(int) file.length()];
            fis.read(data);
            fis.close();
            s = new String(data, CHARSET);
        } catch(IOException e) {
            System.err.println("Exception while READING file " + filePath + " : " + e.getMessage());
        }
        return s;
    }
}
