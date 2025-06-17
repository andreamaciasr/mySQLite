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
        parser.parseQuery();
        QueryExecutor queryExecutor = new QueryExecutor(parser.queries, parser.tokens);
        queryExecutor.getResultsTable();
    }
}
