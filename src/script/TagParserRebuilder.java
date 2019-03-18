package script;

import java.io.*;
import java.util.ArrayList;

public class TagParserRebuilder {
    public static String input_DIR = "./dataset/TagDataset/0_origin/";//default = ./dataset/TagDataset/0_origin/
    public static String output_DIR = "./dataset/TagDataset/";
    public static int N = 10;
    public static File[] inputFiles;
    public static File[] f_out = new File[N];
    public static BufferedWriter[] b_out = new BufferedWriter[N];

    public static void main(String[] args) {
        System.exit(1); //Per sicurezza
        System.out.println("process --> START");
        inizializeOutputFiles();
        try {
            BufferedReader b_in;
            int step = 0;
            //Ciclo su tutti i file di input
            for (File f : inputFiles) {
                int percentage = 100 / inputFiles.length * step;
                System.out.println(percentage + "% -> Parsing file " + f.getPath());
                b_in = new BufferedReader(new FileReader(f));

                String readLine = "", printLine = "";
                //Ciclo su tutte le righe del file
                while ((readLine = b_in.readLine()) != null) {
                    try {
                        int filenum = getNumFile(readLine);
                        if (filenum >= 0 && filenum < N) {
                            printLine = getUrl(readLine) + '\t' + getLanguages(readLine) + '\t' + getcharset(readLine) + "\n";
                            b_out[filenum].write(printLine);
                        }
                    }catch(Exception e){
                        System.out.println("Error skipped, errorcode:" + e.getMessage());
                    }
                }
                step++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        closeAll();
        System.out.println("process --> END");
    }

    public static void inizializeOutputFiles(){
        try {
            //Preparazione file di lettura
            File folder = new File(input_DIR);
            inputFiles = folder.listFiles();

            //Preparazione file di scrittura
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


    public static void closeAll(){
        try {
            for (BufferedWriter bw : b_out) {
                bw.close();
            }
        }catch (Exception e){
            System.out.println("CloseAll exception: " + e.getMessage());
        }
    }

    public static int getNumFile(String line) throws Exception{
        int refIndex = line.indexOf(".warc.gz\"");
        int num = Integer.parseInt(line.substring(refIndex-5, refIndex));
        return num;
    }

    /**
     * "url": "http://5.26.118.0:82/index.php/BBB/article/view/101"
     * @param line
     * @return
     */
    public static String getUrl(String line) throws Exception{
        int startIndex = line.indexOf("{\"url\": \"") + 9;
        int endIndex = line.indexOf("\"", startIndex);
        return line.substring(startIndex, endIndex);
    }

    /**
     * "languages": "tur"
     * @param line
     * @return
     */
    public static String getLanguages(String line) throws Exception{
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
    public static String getcharset(String line) throws Exception{
        if(!line.contains("\"charset\": \""))
            return "-";
        int startIndex = line.indexOf("\"charset\": \"") + 12;
        int endIndex = line.indexOf("\"", startIndex);
        return line.substring(startIndex, endIndex);
    }
}
