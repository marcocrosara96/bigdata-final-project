package script;

import map.Dictionary;
import map.JsonParser;

public class JsonParserTest {
    public static void main(String[] args) {
        Dictionary d = JsonParser.loadDictionary100("./dataset/Dictionary/dictionary_100.json");
        System.out.println(d);
    }
}
