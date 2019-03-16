package script;

import java.io.*;
import java.nio.file.StandardOpenOption;
import java.util.Scanner;
import java.util.regex.Pattern;

public class TagParserRebuilder {
    public static String input_PATH = "./dataset/TagDataset/0_origin/cdx-00000";
    public static String output_DIR = "./dataset/TagDataset/";
    //public static String pattern = "-0000\\d.warc.gz";
    public static int N = 10;
    public static File[] f_out = new File[N];
    public static BufferedWriter[] b_out = new BufferedWriter[N];

    public static void main(String[] args) {
        inizializeOutputFiles();
        try {
            File f_in = new File(input_PATH);

            BufferedReader b_in = new BufferedReader(new FileReader(f_in));

            String readLine = "", printLine = "";
            while ((readLine = b_in.readLine()) != null) {
                int filenum = getNumFile(readLine);
                if(filenum >= 0 && filenum < N){
                    printLine = getUrl(readLine) + '\t' + getLanguages(readLine) + '\t' + getcharset(readLine) + "\n";
                    b_out[filenum].write(printLine);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void inizializeOutputFiles(){
        try {
            for(int i = 0; i < N; i++){
                String filename = output_DIR + "info-0000" + i + ".info";
                f_out[i] = new File(filename);
                if(!f_out[i].exists()){
                        f_out[i].createNewFile();
                }

                //inizializzo i bufferedWriter
                b_out[i] = new BufferedWriter(new FileWriter(f_out[i], true));
            }
        } catch (Exception e) {
            System.out.println("Errore inizializzazione file " + e.getMessage());
        }
    }

    public static int getNumFile(String line){
        int refIndex = line.indexOf(".warc.gz\"");
        int num = Integer.parseInt(line.substring(refIndex-5, refIndex));
        return num;
    }

    /**
     * "url": "http://5.26.118.0:82/index.php/BBB/article/view/101"
     * @param line
     * @return
     */
    public static String getUrl(String line){
        int startIndex = line.indexOf("{\"url\": \"") + 9;
        int endIndex = line.indexOf("\"", startIndex);
        return line.substring(startIndex, endIndex);
    }

    /**
     * "languages": "tur"
     * @param line
     * @return
     */
    public static String getLanguages(String line){
        if(!line.contains("\"languages\": \""))
            return "-";
        int startIndex = line.indexOf("\"languages\": \"") + 14;
        int endIndex = line.indexOf("\"", startIndex);
        return line.substring(startIndex, endIndex);
    }

    /**
     * "charset": "UTF-8"
     * @param line
     * @return
     */
    public static String getcharset(String line){
        if(!line.contains("\"charset\": \""))
            return "-";
        int startIndex = line.indexOf("\"charset\": \"") + 12;
        int endIndex = line.indexOf("\"", startIndex);
        return line.substring(startIndex, endIndex);
    }
}
