//SELECT movies.title, actors.name
//FROM movies
//INNER JOIN actors ON movies.id = actors.movie_id;


import java.util.ArrayList;
import java.util.HashMap;
import com.opencsv.CSVReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class ParseQuery {

    String[] keywords = {"SELECT", "INSERT INTO", "VALUES", "UPDATE", "SET", "DELETE", "FROM", "WHERE", "JOIN", "ON"};
    String demo = "SELECT * FROM file.csv WHERE columnName = value";


    public String[] getUserQueryFromCLI() {
        System.out.println("Enter a query: ");
        Scanner scanner = new Scanner(System.in);
        String query = scanner.nextLine();
        return query.split(" ");
    };


//    public static List<HashMap<String, String>> createTableFromCSV (String filePath){
//        List<HashMap <String, String>> table = new ArrayList<>();
//        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
//            String[] headers = reader.readNext();
//            System.out.println(Arrays.toString(headers));
//            String[] line;
//            while ((line = reader.readNext()) != null) {
//                HashMap<String, String> row = new HashMap<>();
//                for (int i = 0; i < headers.length; i++) {
//                    row.put(headers[i], line[i]);
//                }
//                table.add(row);
//            }
//        } catch (IOException | com.opencsv.exceptions.CsvValidationException e) {
//            e.printStackTrace();
//        }
//        return table;
//    }

    public static List<LinkedHashMap<String, String>> createTableFromCSV(String filePath) {
        List<LinkedHashMap<String, String>> table = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            String[] headers = reader.readNext();
            System.out.println(Arrays.toString(headers));
            String[] line;
            while ((line = reader.readNext()) != null) {
                LinkedHashMap<String, String> row = new LinkedHashMap<>();
                for (int i = 0; i < headers.length; i++) {
                    row.put(headers[i], line[i]);
                }
                table.add(row);
            }
        } catch (IOException | com.opencsv.exceptions.CsvValidationException e) {
            e.printStackTrace();
        }
        return table;
    }

    public static void main(String[] args) {
        List<LinkedHashMap<String, String>>  table = createTableFromCSV("test_data/test_movies.csv");
        System.out.println(table);
    }


}
