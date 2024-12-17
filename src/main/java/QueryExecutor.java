import java.util.*;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryExecutor {

    final List<LinkedHashMap<String, Object>> table1;
    final List<LinkedHashMap<String, Object>> table2;
    final HashMap<String, Object> queriesMap;
    final String[] queryArr;
    final String firstCommand;
    final String firstTableName;
    final String secondTableName;
    final String[] WHEREArgs;
    final String[] SELECTArgs;

    final String[] table1Headers;
    final String[] table2Headers;
    Map<String, BiFunction<Object, Object, Boolean>> whereOperations = new HashMap<>();
    CSVTable csvTable1 = new CSVTable();
    CSVTable csvTable2 = new CSVTable();


    public QueryExecutor(HashMap<String, Object> queriesMap, String[] queryArr) {
        csvTable1.createTableFromCSV((String) queriesMap.get("FROM"));
        this.table1 = csvTable1.getTable();
        this.table1Headers = csvTable1.getColumnsNames();

        csvTable2.createTableFromCSV((String) queriesMap.get("JOIN"));
        this.table2 = csvTable2.getTable();
        this.table2Headers = csvTable2.getColumnsNames();

        this.queriesMap = queriesMap;
        this.queryArr = queryArr;
        this.firstCommand = queryArr[0];
        this.firstTableName = extractTableName((String) this.queriesMap.get("FROM")); // remove .csv from tableName.csv
        this.secondTableName = extractTableName((String) this.queriesMap.get("JOIN"));  // remove .csv from tableName.csv
        this.WHEREArgs = queriesMap.get("WHERE") != null ? (String[]) queriesMap.get("WHERE") : null;
        this.SELECTArgs = queriesMap.get("SELECT") != null ? (String[]) this.queriesMap.get("SELECT") : null;

        addWhereOperation("=", (left, right) -> compare(left, right) == 0);
        addWhereOperation("!=", (left, right) -> compare(left, right) != 0);
        addWhereOperation("<", (left, right) -> compare(left, right) < 0);
        addWhereOperation(">", (left, right) -> compare(left, right) > 0);
        addWhereOperation("<=", (left, right) -> compare(left, right) <= 0);
        addWhereOperation(">=", (left, right) -> compare(left, right) >= 0);

        System.out.println("firstTableName: " + firstTableName);
        System.out.println("SecondTableName: " + secondTableName);
        System.out.printf("table1: %s\n", table1);
        System.out.printf("table2: %s\n", table2);

    }

    private void addWhereOperation(String operator, BiFunction<Object, Object, Boolean> function) {
        whereOperations.put(operator, function);
    }

    private int compare(Object left, Object right) {
        if (left instanceof String && right instanceof String) {
            return ((String) left).compareTo((String) right);
        } else if (left instanceof Integer && right instanceof Integer) {
            return ((Integer) left).compareTo((Integer) right);
        } else if (left instanceof String && right instanceof Integer) {
            return ((String) left).compareTo(String.valueOf(right));
        } else if (left instanceof Integer && right instanceof String) {
            return Integer.compare((Integer) left, Integer.parseInt((String) right));
        } else {
            throw new IllegalArgumentException("Unsupported types on WHERE condition");

        }
    }


public String extractTableName(String filePath) {
    String regex = ".*/(.*?)\\.csv$|^(.*?)\\.csv$";
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(filePath);
    if (matcher.find()) {
        return matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
    }
    return null;
}


public static Integer tryParseInt(String value) {
    try {
        return Integer.parseInt(value); // Return the parsed integer
    } catch (NumberFormatException e) {
        return null; // Return null if parsing fails
    }
}

public Object processTableField(LinkedHashMap<String, Object> entry, String field, String[] tableHeaders) {
    if (Arrays.asList(tableHeaders).contains(field)) {
        return entry.get(field);
    } else {
        System.out.println("Non-existent column name");
        return null;
    }
}

private boolean isQuotedString(String str) {
    return (str.startsWith("\"") && str.endsWith("\"")) ||
            (str.startsWith("'") && str.endsWith("'"));
}

public Object parseWHEREArgument(String argument, LinkedHashMap<String, Object> entry1, LinkedHashMap<String, Object> entry2) {
    // check for a number
    Integer number = tryParseInt(argument);
    if (number != null) return number;

    // check for a String
    if (isQuotedString(argument)) {
        //return argument minus the quotes
        return argument.substring(1, argument.length() - 1);
    }

    // check for format 'table.field'
    String[] parts = argument.split("\\.");
    if (parts.length == 2) {
        String table = parts[0];
        String field = parts[1];

        if (table.equals(firstTableName)) {
            return processTableField(entry1, field, table1Headers);
        } else if (table.equals(secondTableName)) {
            return processTableField(entry2, field, table2Headers);
        } else {
            System.out.println("Table not found");
            return null;
        }

    }
    return null;
}


public Boolean doWHERE(LinkedHashMap<String, Object> table1, LinkedHashMap<String, Object> table2) {
    if (queriesMap.get("WHERE") != null) {

        Object left = parseWHEREArgument(this.WHEREArgs[0], table1, table2); // this checks for formats: table.field, 'String' or Integer and return the value ready to be passed to function
        Object right = parseWHEREArgument(this.WHEREArgs[2], table1, table2);
        String operator = this.WHEREArgs[1];

        BiFunction<Object, Object, Boolean> fn = whereOperations.get(operator);
        System.out.println("Function for Where: " + operator);
        System.out.println("left: " + left);
        System.out.printf("right: %s\n", right);

        if (fn != null) {
            System.out.println("function is not null");
            return fn.apply(left, right);
        } else {
            System.out.println("Wrong operator passed to WHERE");
            return null;
        }
    }
    return null;
}


// getField gets the fields when passed the whole argument string, like "movies.director_id = directors.id"
public String getField(String ConditionString, String tableName) {
    String regex = tableName + "\\.(\\w+)";
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(ConditionString);
    if (matcher.find()) {
        return matcher.group(1);
    }
    // add something for unmatched table name, try catch block
    return null;
}

public String removeEverythingAfterDot(String tableName) {
    String[] parts = tableName.split("\\.");
    return parts[0];
}

public String removeEverythingBeforeDot(String tableName) {
    String[] parts = tableName.split("\\.");
    return parts[1];
}

public List<LinkedHashMap<String, Object>> runQueryViaFirstCommand() {
    List<LinkedHashMap<String, Object>> resultsTable = new ArrayList<>();
    switch (this.firstCommand) {
        case "SELECT" -> {
            if (queriesMap.get("JOIN") != null) {
                String ONConditionAsString = (String) this.queriesMap.get("ON"); // "movies.director_id = directors.id"
                String ONTable1Field = getField(ONConditionAsString, this.firstTableName);
                String ONTable2Field = getField(ONConditionAsString, this.secondTableName);
                System.out.println("ONConditionAsString: " + ONConditionAsString);

                resultsTable = getSelectWithJoin(table1, table2, ONTable1Field, ONTable2Field);

                return resultsTable;
            }
        }
        case "INSERT INTO" -> {
            return resultsTable;
        }
        case "UPDATE" -> {
            return resultsTable;
        }
        case "DELETE" -> {
            return resultsTable;
        }
    }
    return resultsTable;
}


public void doSELECT(LinkedHashMap<String, Object> entry, String tableName, String[]
        SELECTArgs, LinkedHashMap<String, Object> newEntry) {

//        System.out.println("selectArgs: " + Arrays.toString(SELECTArgs));
//        System.out.println("tableNAme: " + tableName);
    for (String arg : SELECTArgs) { // SELECT movies.name, directors.name
        // regex to divide table and field from table.field
        String table = removeEverythingAfterDot(arg); // movies
        String field = removeEverythingBeforeDot(arg); // title

        if (Objects.equals(table, tableName)) {
            // extract value from the field on the entry
            Object value = entry.get(field); // {title: "something", release_year: "1998", director: something, id: 1}
            if (newEntry.containsKey(field)) {
                // pass arg that contains the tableName to create unique keys
                newEntry.put(arg, value);
            } else {
                newEntry.put(field, value);
            }

        }
    }
}


public List<LinkedHashMap<String, Object>> getSelectWithJoin
        (List<LinkedHashMap<String, Object>> table1, List<LinkedHashMap<String, Object>> table2, String
                field1, String field2) {
    List<LinkedHashMap<String, Object>> resultsTable = new ArrayList<>();
    LinkedHashMap<String, Object> newEntry = new LinkedHashMap<>();

    for (LinkedHashMap<String, Object> entry1 : table1) {
        for (LinkedHashMap<String, Object> entry2 : table2) {
            if (entry2.get(field2).equals(entry1.get(field1))) {   // (entry1[field1] == entry2[field2])  movies.director_id = directors.id; JOIN
                // handle WHERE
                if (this.WHEREArgs != null) {
                    if (doWHERE(entry1, entry2)) {
                        System.out.println("Match found on WHERE condition");
                        doSELECT(entry1, firstTableName, SELECTArgs, newEntry);
                        doSELECT(entry2, secondTableName, SELECTArgs, newEntry);
                        resultsTable.add(newEntry);
                    }
                } else {
                    doSELECT(entry1, firstTableName, SELECTArgs, newEntry);
                    doSELECT(entry2, secondTableName, SELECTArgs, newEntry);
                    resultsTable.add(newEntry);
                }
            }

        }
        newEntry = new LinkedHashMap<>();
    }

    return resultsTable;
}

}
