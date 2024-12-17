import java.util.ArrayList;
import java.util.HashMap;

public class QueryParser {

    HashMap<String, Object> queries;


    public QueryParser(String query) {
        queries = new HashMap<>();
        queries.put("SELECT", new ArrayList<String>()); // ["movies.title", "directors.name"]
        queries.put("INSERT INTO", "");
        queries.put("VALUES", new ArrayList<String>());
        queries.put("UPDATE", new ArrayList<String>());
        queries.put("SET", new ArrayList<String>());
        queries.put("DELETE", new ArrayList<String>());
        queries.put("FROM", "");
        queries.put("WHERE", new ArrayList<String>());
        queries.put("JOIN", "");
        queries.put("ON", ""); // movies.director_id = directors.id

    }

//
    public HashMap<String, Object> getQueries() {
        return queries;
    }




}