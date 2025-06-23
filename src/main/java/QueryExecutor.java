import java.util.*;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryExecutor {

    List<LinkedHashMap<String, Object>> table1;
    List<LinkedHashMap<String, Object>> table2;
    final HashMap<String, Object> queriesMap;
    final ArrayList<String> queryArr;
    String firstCommand;
    String firstTableName;
    String secondTableName;
    ArrayList<String> WHEREArgs;
    ArrayList<String> SELECTArgs;

    String[] table1Headers;
    String[] table2Headers;
    Map<String, BiFunction<Object, Object, Boolean>> whereOperations = new HashMap<>();
    CSVTable csvTable1 = new CSVTable();
    CSVTable csvTable2 = new CSVTable();

    public QueryExecutor(HashMap<String, Object> queriesMap, ArrayList<String> queryArr) {
        this.queriesMap = queriesMap;
        this.queryArr = queryArr;

        initializeTables();
        initializeQueryFields();
        initializeWhereOperations();
    }

    private void initializeTables() {
        // "FROM" and "JOIN" are the keys in the queriesMap that contain the file paths to the CSV files
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
            // If there is no JOIN, we set the second table to an empty list and headers to an empty array
            this.table2Headers = new String[0];
        }
    }

    private void initializeQueryFields() {
        this.firstCommand = queryArr.getFirst();
        this.firstTableName = extractTableName((String) this.queriesMap.get("FROM"));
        this.secondTableName = extractTableName((String) this.queriesMap.get("JOIN"));
        this.WHEREArgs = (queriesMap.get("WHERE") != null && !((ArrayList<String>) queriesMap.get("WHERE")).isEmpty())
                ? (ArrayList<String>) queriesMap.get("WHERE")
                : null;
        this.SELECTArgs = queriesMap.get("SELECT") != null
                ? (ArrayList<String>) this.queriesMap.get("SELECT")
                : null;
    }

    private void initializeWhereOperations() {
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

    // method to cast data types to be able to use the compare method
    private int compare(Object left, Object right) {
        // one value from WHERE can be an integer if it comes from the table (ex: table.id),
        // if it comes from the query, it is always a String
        return switch (left) {
            case String s when right instanceof String -> s.compareTo((String) right);
            case Integer i when right instanceof Integer ->
                    Integer.compare(i, (Integer) right);
            case String s when right instanceof Integer -> Integer.compare(Integer.parseInt(s), (Integer) right);
            case Integer i when right instanceof String -> Integer.compare(i, Integer.parseInt((String) right));
            case null, default -> throw new IllegalArgumentException("Unsupported types on WHERE condition");
        };
    }

    // this method extracts the table name from the file path
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

    public Object getColumnValueForWHEREOperation(LinkedHashMap<String, Object> row, String column, String[] tableHeaders) {
        if (Arrays.asList(tableHeaders).contains(column)) {
            return row.get(column);
        } else {
            System.out.println("Non-existent column name");
            return null;
        }
    }

    private boolean isQuotedString(String str) {
        return (str.startsWith("\"") && str.endsWith("\"")) ||
                (str.startsWith("'") && str.endsWith("'"));
    }

    public Object parseWHEREArgument(String argument, LinkedHashMap<String, Object> row1, LinkedHashMap<String, Object> row2) {
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
            String column = parts[1];

            if (table.equals(firstTableName)) {
                return getColumnValueForWHEREOperation(row1, column, table1Headers);
            } else if (table.equals(secondTableName)) {
                return getColumnValueForWHEREOperation(row2, column, table2Headers);
            } else {
                System.out.println("Table not found");
                System.exit(1);
            }
        }
        return null;
    }


    public Boolean doWHERE(LinkedHashMap<String, Object> table1, LinkedHashMap<String, Object> table2) {
        if (this.queryArr.contains("WHERE") && !((ArrayList<?>) queriesMap.get("WHERE")).isEmpty()) {

            // this checks for formats: table.field, 'String' or Integer and returns the value ready to be passed to the function
            Object left = parseWHEREArgument(this.WHEREArgs.getFirst(), table1, table2);
            Object right = parseWHEREArgument(this.WHEREArgs.get(2), table1, table2);
            // this checks for the operator, ex: '=', '!=', '<'
            String operator = this.WHEREArgs.get(1);

            // get the function that corresponds to the operator
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


    // gets the column name from the table when passed a string like "movies.director_id = directors.id"
    public String getColumn(ArrayList<String> conditionStrings, String tableName) {
        String conditionString = String.join(" ", conditionStrings);
        String regex = tableName + "\\.(\\w+)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(conditionString);
        if (matcher.find()) {
            return matcher.group(1);
        }
        System.out.println("Table name not found: " + conditionString);
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
                    String ONTable1Column = getColumn(ONConditionAsString, this.firstTableName);
                    String ONTable2Column = getColumn(ONConditionAsString, this.secondTableName);

                    resultsTable = getSelectWithJoin(table1, table2, ONTable1Column, ONTable2Column);

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


    public void doSELECT(LinkedHashMap<String, Object> row, String tableName, ArrayList<String>
            SELECTArgs, LinkedHashMap<String, Object> newRow) {
        if (Objects.equals(SELECTArgs.getFirst(), "*")) {
            newRow.putAll(row);
            return;
        }
        for (String arg : SELECTArgs) { // SELECT movies.name, directors.name
            // regex to divide table and column from table.column
            String table = removeEverythingAfterDot(arg); // movies
            String column = removeEverythingBeforeDot(arg); // title

            if (Objects.equals(table, tableName)) {
                // extract value from the column on the row
                Object value = row.get(column); // {title: "something", release_year: "1998", director: something, id: 1}
                if (newRow.containsKey(column)) {
                    // pass arg that contains the tableName to create unique keys
                    newRow.put(arg, value);
                } else {
                    newRow.put(column, value);
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
                    column1, String column2) {
        List<LinkedHashMap<String, Object>> resultsTable = new ArrayList<>();
        LinkedHashMap<String, Object> newEntry = new LinkedHashMap<>();

        for (LinkedHashMap<String, Object> row1 : table1) {
            for (LinkedHashMap<String, Object> row2 : table2) {
                if (row2.get(column2).equals(row1.get(column1))) {   // (row1[column1] == row2[column2])
                    if (this.WHEREArgs != null && !this.WHEREArgs.isEmpty()) {

                        if (doWHERE(row1, row2)) {
                            doSELECT(row1, firstTableName, SELECTArgs, newEntry);
                            doSELECT(row2, secondTableName, SELECTArgs, newEntry);
                            resultsTable.add(newEntry);
                        }
                    } else {
                        doSELECT(row1, firstTableName, SELECTArgs, newEntry);
                        doSELECT(row2, secondTableName, SELECTArgs, newEntry);
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

    public void getResultsTable() {
        List<LinkedHashMap<String, Object>> resultsTable = runQueryViaFirstCommand();
        formatResultsTable(resultsTable);
    }

}
