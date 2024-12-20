//SELECT movies.title, actors.name
//FROM movies
//INNER JOIN actors ON movies.id = actors.movie_id;


import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.*;
import java.util.function.Function;


public class MySqlite {

    public static void main(String[] args) {
        MySqlite mySqlite = new MySqlite();
//        String query = mySqlite.getUserQueryFromCLI();

//        QueryParser queryParser = new QueryParser(query);
//        -----------> uncomment when parser works
//        HashMap<String, Object> queriesMap = queryParser.getQueries(); // query parser returns the queryMap Object

//        String table1path = (String) queriesMap.get("FROM");
////        String table2path = (String) queriesMap.get("ON");
//        String [][] testWhere = {
//                {"movies_demo.title", "=", "'Selma'"},
//                {"movies_demo.genre", "=", "'Drama'"},
//                {"movies_demo.year", "=", "2014"}, // not in the table
//                {"directors_demo.name", "=", "'Ava DuVernay'"},
//                {"directors_demo.id", "=", "5"}
//        };
//        // -----------> hardcoding before parser works. Delete after.
//        String table1path = "test_data/movies_demo.csv";
//        String table2path = "test_data/directors_demo.csv";
//        String[] SELECTArr = {"movies_demo.title", "directors_demo.name"};
//        String FROM = table1path;
//        String JOIN = table2path;
//        String[] WHERE = testWhere[3]; // fix the split
//        String ON = "movies_demo.director_id == directors_demo.id";
//        String[] queryArr = "SELECT movies.title, directors.name FROM \"test_data/movies_demo.csv\" JOIN \"test_data/directors_demo.csv\" ON movies.director_id = directors.id".split(" ");

        // array of WHERE arguments to test:


//        HashMap<String, Object> queriesMap = new HashMap<>();
//        queriesMap.put("SELECT", new ArrayList<String>()); // movies.title, directors.name;
//        queriesMap.put("INSERT INTO", "");
//        queriesMap.put("VALUES", new ArrayList<String>());
//        queriesMap.put("UPDATE", new ArrayList<String>());
//        queriesMap.put("SET", new ArrayList<String>());
//        queriesMap.put("DELETE", new ArrayList<String>());
//        queriesMap.put("FROM", ""); // movies.csv
//        queriesMap.put("WHERE", new ArrayList<String>()); // movies.genre = 'Sci-Fi';
//        queriesMap.put("JOIN", ""); // directors
//        queriesMap.put("ON", ""); // movies.director_id = directors.id;



        // ------------->

        QueryParser parser = new QueryParser();
        String query = parser.getUserQueryFromCLI();
        parser.tokens = parser.makeArrayQuery(query);
        ArrayList<Pair<String, Integer>> pairsArray = parser.makeTokenPairList();
        parser.extractArguments(pairsArray);
        HashMap<String, Object> queriesMap = parser.queries;
        System.out.println("queriesMap: " + queriesMap);
        QueryExecutor queryExecutor = new QueryExecutor(queriesMap, parser.tokens);
        List<LinkedHashMap<String, Object>> resultsTable = queryExecutor.runQueryViaFirstCommand();

        System.out.println(resultsTable);


    }

}

//SELECT movies.title, directors.name
//FROM movies
//JOIN directors ON movies.director_id = directors.id
//WHERE movies.genre = 'Drama'