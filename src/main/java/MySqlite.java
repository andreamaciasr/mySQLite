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
        mySqlite.run();

    }

    public void run() {
        QueryParser parser = new QueryParser();
        String query = parser.getUserQueryFromCLI();
        parser.tokens = parser.makeArrayQuery(query);
        ArrayList<Pair<String, Integer>> pairsArray = parser.makeTokenPairList();
        parser.extractArguments(pairsArray);
        HashMap<String, Object> queriesMap = parser.queries;
        QueryExecutor queryExecutor = new QueryExecutor(queriesMap, parser.tokens);
        List<LinkedHashMap<String, Object>> resultsTable = queryExecutor.runQueryViaFirstCommand();
        queryExecutor.formatResultsTable(resultsTable);
    }

}
