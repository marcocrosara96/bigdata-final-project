package script;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

public class ReaderTest {
    public static final String filePath = "./dataset/data/cdx-00000";

    public static void main(String[] args) {
        //showLines();
        //System.out.println(Analyzer.Map.class);
    }

    public static void searchLine(){
        String searchline;
        Scanner scanner = new Scanner(System.in);
        System.out.print("from_line:");
        searchline = scanner.nextLine();

        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(filePath));
            String line = reader.readLine();
            while (line != null) {
                if(line.contains(searchline)) {
                    System.out.println(line);
                    break;
                }
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void showLines(){
        int from, to;
        Scanner scanner = new Scanner(System.in);
        System.out.print("from_line:");
        from = Integer.parseInt(scanner.nextLine());
        System.out.print("to_line:");
        to = Integer.parseInt(scanner.nextLine());

        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(filePath));
            String line = reader.readLine();
            int i = 0;
            while (i <= to && line != null) {
                if(i >= from)
                    System.out.println(line);
                line = reader.readLine();
                i++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
