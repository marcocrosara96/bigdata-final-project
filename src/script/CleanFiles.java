package script;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class CleanFiles {
    public static HashMap<String, String> lang = new HashMap<>();

    public static void main(String[] args) {
        load();
        completeDictionary();
    }

    public static void completeDictionary(){
        BufferedReader reader;
        String s = "";
        try {
            reader = new BufferedReader(new FileReader("./dataset/Dictionary/dictionary.json"));
            String line = reader.readLine();
            while (line != null) {
                s += line;
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        int i = 20;
        int j = 0;
        while (i < s.length() && j < 190) {
            i = s.indexOf("\"name\": \"", i) + 9;
            String name = s.substring(i, s.indexOf("\"", i));
            i += name.length() + 1;
            //-------------------------------------
            int k = s.indexOf("\"ISO_639_V2\": \"", i) + 15;
            String tag = s.substring(k, s.indexOf("\"", k));
            if(tag.equals("***")) {
                s = s.substring(0, k) + lang.get(name) + s.substring(k + 3, s.length());
            }

            //System.out.println(tag);
            //-------------------------------------
            //System.out.println(name);
            j++;
        }
        System.out.println(s);
    }

    public static void load(){
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader("./dataset/Dictionary/new_v2.txt"));
            String line = reader.readLine();
            while (line != null) {
                String[] pair = line.split("\t");
                lang.put(pair[1], pair[0]);
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
