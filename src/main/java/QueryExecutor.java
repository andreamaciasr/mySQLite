import org.apache.commons.collections4.iterators.SkippingIterator;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
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
    final String[] table1Columns;
    final String[] table2Columns;

    final String[] headersTable1;
    final String[] headersTable2;
    Map<String, BiFunction<Object, Object, Boolean>> whereOperations = new HashMap<>();
    CSVTable csvTable1 = new CSVTable();
    CSVTable csvTable2 = new CSVTable();


    public QueryExecutor(HashMap<String, Object> queriesMap, String[] queryArr) {
        csvTable1.createTableFromCSV((String) queriesMap.get("FROM"));
        this.table1 = csvTable1.getTable();
        this.headersTable1 = csvTable1.getColumnsNames();

        csvTable2.createTableFromCSV((String) queriesMap.get("JOIN"));
        this.table2 = csvTable2.getTable();
        this.headersTable2 = csvTable2.getColumnsNames();

        this.queriesMap = queriesMap;
        this.queryArr = queryArr;
        this.firstCommand = queryArr[0];
        this.firstTableName = extractTableName((String) this.queriesMap.get("FROM")); // remove .csv from tableName.csv
        this.secondTableName = extractTableName((String) this.queriesMap.get("JOIN"));  // remove .csv from tableName.csv
        this.WHEREArgs = queriesMap.get("WHERE") != null ? (String[]) queriesMap.get("WHERE") : null;
        this.SELECTArgs = queriesMap.get("SELECT") != null ? (String[]) this.queriesMap.get("SELECT") : null;
        this.table1Columns = null;
        this.table2Columns = null;

        System.out.println("headers: " + Arrays.toString(headersTable1) + " " + Arrays.toString(headersTable2));

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

    public Boolean doWhere(LinkedHashMap<String, Object> entry1, LinkedHashMap<String, Object> entry2) {
        Object leftValue;
        Object rightValue;
        String leftStatementTableName = removeEverythingAfterDot(WHEREArgs[0]);
        String leftStatementFieldName = removeEverythingBeforeDot(WHEREArgs[0]);

        if (Objects.equals(leftStatementTableName, this.firstTableName)) {
            try {
                leftValue = entry1.get(leftStatementFieldName);

            } catch (Exception e) {
                System.out.println("There's no " + leftStatementFieldName + " column on the " + this.firstTableName + " table.");
                throw new RuntimeException(e);
            }
        } else if (Objects.equals(leftStatementTableName, this.secondTableName)) {
            try {

            } catch (Exception e) {
                throw new RuntimeException(e);
                // unknown column name
            }
        } else {
            // unexistent table;
        }


        return true;
    }

    public static Integer tryParseInt(String value) {
        try {
            return Integer.parseInt(value); // Return the parsed integer
        } catch (NumberFormatException e) {
            return null; // Return null if parsing fails
        }
    }

//    public Object getValueForWHERECondition(LinkedHashMap<String, Object> entry1, LinkedHashMap<String, Object> entry2, String argument) {
//        Integer number = tryParseInt(argument);
//        if (number != null) {
//            return number;
//        } else if (argument.charAt(0) == '\"' && argument.charAt(argument.length() - 1) == '\"' || argument.charAt(0) == '\'' && argument.charAt(argument.length() - 1) == '\'') {
//            return argument;
//        } else if (argument.)
//    }


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
                    // Boolean where

                    System.out.println("field1: " + ONTable1Field);
                    System.out.println("field2: " + ONTable2Field);

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


    public void doSELECT(LinkedHashMap<String, Object> entry, String tableName, String[] SELECTArgs, LinkedHashMap<String, Object> newEntry) {

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
        System.out.println("SelectArgs: " + Arrays.toString(SELECTArgs));
//        String[] SELECTArgs = (String[]) this.queriesMap.get("SELECT");
//        Object[] WHEREArgs = (Object[]) this.queriesMap.get("WHERE");

//        if (WHEREArgs != null) {
//            try {
//                Object left = WHEREArgs[0];
//                Object right = WHEREArgs[2];
//                Object operator = WHEREArgs[1];
//                BiFunction<Object, Object, Boolean> fn = whereOperations.get(operator);
//            } catch (Exception e) {
//                System.out.println("Invalid WHERE condition");
//
//            }

        for (LinkedHashMap<String, Object> entry1 : table1) {
            for (LinkedHashMap<String, Object> entry2 : table2) {
                if (entry2.get(field2).equals(entry1.get(field1))) {   // (entry1[field1] == entry2[field2])  movies.director_id = directors.id; JOIN
                    // handle where here
//                        if (WHEREArgs != null && fn(left, right) == true) {
                    doSELECT(entry1, firstTableName, SELECTArgs, newEntry);
                    doSELECT(entry2, secondTableName, SELECTArgs, newEntry);
                    resultsTable.add(newEntry);

                }

            }
            newEntry = new LinkedHashMap<>();
        }

        return resultsTable;
    }

}
