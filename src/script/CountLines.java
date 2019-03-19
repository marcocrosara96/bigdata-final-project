package script;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CountLines {
    public static void main(String[] args) {
        BufferedReader reader;
        int sum = 0;
        try {
            reader = new BufferedReader(new FileReader("/home/mark/Desktop/Progetti/bigdata-final-project/dataset/CommonCrawl/00000.wet"));
            String line = reader.readLine();
            while (line != null) {
                if(line.contains("WARC/1.0")) {
                    sum++;
                }
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(sum);
    }
}
