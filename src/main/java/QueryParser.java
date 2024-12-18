import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    public ArrayList<String> makeArrayQuery(String query) {
        // replace commas with spaces to break them apart
        query = query.replace(",", " ");

        // split by spaces, ignore spaces within quotes
        String regex = "\"([^\"]*)\"|'([^']*)'|\\S+";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(query);

        ArrayList<String> tokens = new ArrayList<>();
        while (matcher.find()) {
            if (matcher.group(1) != null) {
                tokens.add("\"" + matcher.group(1) + "\""); // add double-quoted string with quotes
            } else if (matcher.group(2) != null) {
                tokens.add("'" + matcher.group(2) + "'"); // add single-quoted string with quotes
            } else {
                tokens.add(matcher.group());
            }
        }
        return tokens;
    }

    public static void main(String[] args) {
        String query = "SELECT movies.name AS movie,  directors.name AS director FROM movies JOIN directors ON movies.director_id = directors.id WHERE directors.name = 'Greta Gerwig'";
        QueryParser parser = new QueryParser(query);
        ArrayList<String> tokens = parser.makeArrayQuery(query);
        System.out.println(tokens);
    }

    public HashMap<String, Object> getQueries() {
        return queries;
    }




}