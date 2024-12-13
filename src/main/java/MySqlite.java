//SELECT movies.title, actors.name
//FROM movies
//INNER JOIN actors ON movies.id = actors.movie_id;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.*;


public class MySqlite {

    //String[] keywords = {"SELECT", "INSERT INTO", "VALUES", "UPDATE", "SET", "DELETE", "FROM", "WHERE", "JOIN", "ON"};
   // String demo = "SELECT * FROM file.csv WHERE columnName = value";

    public String getUserQueryFromCLI() {
        System.out.println("Enter a query: ");
        Scanner scanner = new Scanner(System.in);
        return scanner.nextLine();
    };

    public static void main(String[] args) {
        MySqlite mySqlite = new MySqlite();
//        String query = mySqlite.getUserQueryFromCLI();

//        QueryParser queryParser = new QueryParser(query);
//        -----------> uncomment when parser works
//        HashMap<String, Object> queriesMap = queryParser.getQueries(); // query parser returns the queryMap Object

//        String table1path = (String) queriesMap.get("FROM");
//        String table2path = (String) queriesMap.get("ON");

        // -----------> hardcoding before parser works. Delete after.
        String table1path = "test_data/movies_demo.csv";
        String table2path = "test_data/directors_demo.csv";
        String[] SELECTArr = {"movies_demo.title", "directors_demo.name"};
        String FROM = table1path;
        String JOIN = table2path;
        String ON = "movies_demo.director_id == directors_demo.id";
        String[] queryArr = "SELECT movies.title, directors.name FROM \"test_data/movies_demo.csv\" JOIN \"test_data/directors_demo.csv\" ON movies.director_id = directors.id".split(" ");



        HashMap<String, Object> queriesMap = new HashMap<>();
        queriesMap.put("SELECT", new ArrayList<String>()); // movies.title, directors.name;
        queriesMap.put("INSERT INTO", "");
        queriesMap.put("VALUES", new ArrayList<String>());
        queriesMap.put("UPDATE", new ArrayList<String>());
        queriesMap.put("SET", new ArrayList<String>());
        queriesMap.put("DELETE", new ArrayList<String>());
        queriesMap.put("FROM", ""); // movies.csv
        queriesMap.put("WHERE", new ArrayList<String>()); // movies.genre = 'Sci-Fi';
        queriesMap.put("JOIN", ""); // directors
        queriesMap.put("ON", ""); // movies.director_id = directors.id;

        queriesMap.put("SELECT", SELECTArr);
        queriesMap.put("FROM", FROM);
        queriesMap.put("JOIN", JOIN);
        queriesMap.put("ON", ON);

        // ------------->

        CSVTable csvTable = new CSVTable();
        System.out.println(queriesMap);
//        List<LinkedHashMap<String, String>> table1 = csvTable.createTableFromCSV(table1path);
//        List<LinkedHashMap<String, String>> table2 = Objects.equals(table2path, "") ? null : csvTable.createTableFromCSV(table2path);
//        System.out.println("Table 1: " + table1);
//        System.out.println("Table 2: " + table2);
//        System.out.println("Query Array: " + Arrays.toString(queryArr));

        QueryExecutor queryExecutor = new QueryExecutor(queriesMap, queryArr);
        List<LinkedHashMap<String, String>> resultsTable = queryExecutor.runQueryViaFirstCommand();




        //QueryExecutor queryExecutor = new QueryExecutor(table1, table2, queriesMap, queryArr);
       // runQueryViaFirstCommand should return the final results;
       // List<LinkedHashMap<String, String>> resultsTable = queryExecutor.runQueryViaFirstCommand();


    }

}
