import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class QueryParser {

    HashMap<String, Object> queries;
    public ArrayList<String> tokens;

    public QueryParser() {
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
        queries.put("ON", new ArrayList<String>()); // movies.director_id = directors.id
    }

    public String getUserQueryFromCLI() {
        System.out.println("Enter a query: ");
        Scanner scanner = new Scanner(System.in);
        String query = scanner.nextLine();
        if (query.trim().isEmpty()) {
            System.out.println("Query cannot be empty");
            System.exit(1);
        }
        return query;
    }

    public ArrayList<String> makeArrayQuery(String query) {
        // replace commas with spaces
        query = query.replace(",", " ");

        // replace curly quotes with straight quotes
        query = query.replace("“", "\"").replace("”", "\"");

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

    public ArrayList<Pair<String, Integer>> makeTokenPairList() {
        ArrayList<Pair<String, Integer>> tokenPairs = new ArrayList<>();
        for (int i = 0; i < this.tokens.size(); i++) {
            if (this.queries.get(this.tokens.get(i)) != null) {
                MutablePair<String, Integer> pair = new MutablePair<>(this.tokens.get(i), i);
                tokenPairs.add(pair);
            }
        }
        return tokenPairs;
    }

    public void getMiddleArgs(int start, int end, String keyword) {
        // when a single value is expected
        if (keyword.equals("INSERT INTO") || keyword.equals("FROM") || keyword.equals("JOIN")) {
            this.queries.put(keyword, this.tokens.get(start + 1));
        } else { // when multiple values are expected
            ArrayList<String> args = new ArrayList<>();
            for (int i = start + 1; i < end; i++) {
                args.add(this.tokens.get(i));
            }
            ((ArrayList<String>) this.queries.get(keyword)).addAll(args);
        }
    }

    public void extractArguments(ArrayList<Pair<String, Integer>> tokenPairs) {
        for (int i = 0; i < tokenPairs.size() - 1; i++) {
            String keyword = tokenPairs.get(i).getLeft();
            getMiddleArgs(tokenPairs.get(i).getRight(), tokenPairs.get(i + 1).getRight(), keyword);
        }
        // get last keyword
        String keyword = tokenPairs.getLast().getLeft();
        getMiddleArgs(tokenPairs.getLast().getRight(), this.tokens.size(), keyword);
    }

    public void parseQuery() {
        String query = getUserQueryFromCLI();
        this.tokens = makeArrayQuery(query);
        ArrayList<Pair<String, Integer>> pairsArray = makeTokenPairList();
        extractArguments(pairsArray);
    }

}