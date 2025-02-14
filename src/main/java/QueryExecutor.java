import java.util.*;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryExecutor {

    final List<LinkedHashMap<String, Object>> table1;
    final List<LinkedHashMap<String, Object>> table2;
    final HashMap<String, Object> queriesMap;
    final ArrayList<String> queryArr;
    final String firstCommand;
    final String firstTableName;
    final String secondTableName;
    final ArrayList<String> WHEREArgs;
    final ArrayList<String> SELECTArgs;

    final String[] table1Headers;
    final String[] table2Headers;
    Map<String, BiFunction<Object, Object, Boolean>> whereOperations = new HashMap<>();
    CSVTable csvTable1 = new CSVTable();
    CSVTable csvTable2 = new CSVTable();

    public QueryExecutor(HashMap<String, Object> queriesMap, ArrayList<String> queryArr) {
        csvTable1.createTableFromCSV((String) queriesMap.get("FROM"));
        this.table1 = csvTable1.getTable();
        this.table1Headers = csvTable1.getColumnsNames();

        String joinFilePath = (String) queriesMap.get("JOIN");
        if (joinFilePath != null && !joinFilePath.isEmpty()) {
            csvTable2.createTableFromCSV(joinFilePath);
            this.table2 = csvTable2.getTable();
            this.table2Headers = csvTable2.getColumnsNames();
        } else {
            this.table2 = new ArrayList<>();
            this.table2Headers = new String[0];
        }

        //QueryParser parser = new QueryParser();
        this.queriesMap = queriesMap;
        this.queryArr = queryArr;
        this.firstCommand = queryArr.getFirst();
        this.firstTableName = extractTableName((String) this.queriesMap.get("FROM")); // remove .csv from tableName.csv
        this.secondTableName = extractTableName((String) this.queriesMap.get("JOIN"));  // remove .csv from tableName.csv
        this.WHEREArgs = (queriesMap.get("WHERE") != null && !((ArrayList<String>) queriesMap.get("WHERE")).isEmpty()) ? (ArrayList<String>) queriesMap.get("WHERE") : null;
        this.SELECTArgs = queriesMap.get("SELECT") != null ? (ArrayList<String>) this.queriesMap.get("SELECT") : null;


        addWhereOperation("=", (left, right) -> compare(left, right) == 0);
        addWhereOperation("!=", (left, right) -> compare(left, right) != 0);
        addWhereOperation("<", (left, right) -> compare(left, right) < 0);
        addWhereOperation(">", (left, right) -> compare(left, right) > 0);
        addWhereOperation("<=", (left, right) -> compare(left, right) <= 0);
        addWhereOperation(">=", (left, right) -> compare(left, right) >= 0);

    }

    private void addWhereOperation(String operator, BiFunction<Object, Object, Boolean> function) {
        whereOperations.put(operator, function);
    }

    private int compare(Object left, Object right) {
        return switch (left) {
            case String s when right instanceof String -> s.compareTo((String) right);
            case Integer i when right instanceof Integer ->
                    Integer.compare(Integer.parseInt((String) left), (Integer) right);
            case String s when right instanceof Integer -> Integer.compare(Integer.parseInt(s), (Integer) right);
            case Integer i when right instanceof String -> Integer.compare(i, Integer.parseInt((String) right));
            case null, default -> throw new IllegalArgumentException("Unsupported types on WHERE condition");
        };
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
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return null;
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
                //System.out.println("processTableField(entry1, field, table1Headers): " + processTableField(entry1, field, table1Headers));
                return processTableField(entry1, field, table1Headers);
            } else if (table.equals(secondTableName)) {
                return processTableField(entry2, field, table2Headers);
            } else {
                System.out.println("Table not found");
                System.exit(1);
            }
        }
        return null;
    }


    public Boolean doWHERE(LinkedHashMap<String, Object> table1, LinkedHashMap<String, Object> table2) {
        if (this.queryArr.contains("WHERE") && !((ArrayList<?>) queriesMap.get("WHERE")).isEmpty()) {

            Object left = parseWHEREArgument(this.WHEREArgs.getFirst(), table1, table2); // this checks for formats: table.field, 'String' or Integer and return the value ready to be passed to function
            Object right = parseWHEREArgument(this.WHEREArgs.get(2), table1, table2);
            String operator = this.WHEREArgs.get(1);

            BiFunction<Object, Object, Boolean> fn = whereOperations.get(operator);

            if (fn != null) {
                return fn.apply(left, right);
            } else {
                System.out.println("Wrong operator passed to WHERE");
                System.exit(1);
            }
        }
        return null;
    }


    // getField gets the fields when passed the whole argument string, like "movies.director_id = directors.id"
    public String getField(ArrayList<String> conditionStrings, String tableName) {
        String conditionString = String.join(" ", conditionStrings);
        String regex = tableName + "\\.(\\w+)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(conditionString);
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
                if (queriesMap.get("JOIN") != "") {
                    ArrayList<String> ONConditionAsString = (ArrayList<String>) this.queriesMap.get("ON");// "movies.director_id = directors.id"
                    String ONTable1Field = getField(ONConditionAsString, this.firstTableName);
                    String ONTable2Field = getField(ONConditionAsString, this.secondTableName);

                    resultsTable = getSelectWithJoin(table1, table2, ONTable1Field, ONTable2Field);

                    return resultsTable;
                } else {
                    return getSelect(table1, firstTableName, SELECTArgs);
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


    public void doSELECT(LinkedHashMap<String, Object> entry, String tableName, ArrayList<String>
            SELECTArgs, LinkedHashMap<String, Object> newEntry) {
        if (Objects.equals(SELECTArgs.getFirst(), "*")) {
            newEntry.putAll(entry);
            return;
        }
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

    public List<LinkedHashMap<String, Object>> getSelect(List<LinkedHashMap<String, Object>> table, String tableName, ArrayList<String> SELECTArgs) {
        List<LinkedHashMap<String, Object>> resultsTable = new ArrayList<>();
        LinkedHashMap<String, Object> newEntry = new LinkedHashMap<>();
        for (LinkedHashMap<String, Object> entry : table) {
            if (this.WHEREArgs != null && !this.WHEREArgs.isEmpty()) {
                if (doWHERE(entry, entry)) {
                    doSELECT(entry, tableName, SELECTArgs, newEntry);
                    resultsTable.add(newEntry);
                }
            } else {
                doSELECT(entry, tableName, SELECTArgs, newEntry);
                resultsTable.add(newEntry);
            }
            newEntry = new LinkedHashMap<>();
        }
        return resultsTable;
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
                    if (this.WHEREArgs != null && !this.WHEREArgs.isEmpty()) {

                        if (doWHERE(entry1, entry2)) {
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

    public void formatResultsTable(List<LinkedHashMap<String, Object>> resultsTable) {
        for (LinkedHashMap<String, Object> entry : resultsTable) {
            for (Map.Entry<String, Object> field : entry.entrySet()) {
                System.out.printf("%s: %s\n", field.getKey(), field.getValue());
            }
            System.out.println();
        }
    }

}
