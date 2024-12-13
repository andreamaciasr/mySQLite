import org.apache.commons.collections4.iterators.SkippingIterator;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryExecutor {

    final List<LinkedHashMap<String, String>> table1;
    final List<LinkedHashMap<String, String>> table2;
    final HashMap<String, Object> queriesMap;
    final String[] queryArr;
    final String firstCommand;
    final String firstTableName;
    final String secondTableName;

    CSVTable csvTable = new CSVTable();

    public QueryExecutor(HashMap<String, Object> queriesMap, String[] queryArr) {
        this.table1 = csvTable.createTableFromCSV((String) queriesMap.get("FROM"));
        this.table2 = csvTable.createTableFromCSV((String) queriesMap.get("JOIN"));
        this.queriesMap = queriesMap;
        this.queryArr = queryArr;
        this.firstCommand = queryArr[0];
        this.firstTableName = extractTableName((String) this.queriesMap.get("FROM")); // remove .csv from tableName.csv
        this.secondTableName = extractTableName((String) this.queriesMap.get("JOIN"));  // remove .csv from tableName.csv

        System.out.println("firstTableName: " + firstTableName);
        System.out.println("SecondTableName: " + secondTableName);

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

    public List<LinkedHashMap<String, String>> runQueryViaFirstCommand() {
        List<LinkedHashMap<String, String>> resultsTable = new ArrayList<>();
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

                    System.out.println("resultsTable: " + resultsTable);
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


    public void doSELECT(LinkedHashMap<String, String> entry, String tableName, String[] SELECTArgs, LinkedHashMap<String, String> newEntry) {

//        System.out.println("selectArgs: " + Arrays.toString(SELECTArgs));
//        System.out.println("tableNAme: " + tableName);
        for (String arg : SELECTArgs) {
            // regex to divide table and field from table.field
            String table = removeEverythingAfterDot(arg); // movies
            String field = removeEverythingBeforeDot(arg); // title

            if (Objects.equals(table, tableName)) {
                // extract value from the field on the entry
                String value = entry.get(field); // {title: "something", release_year: "1998", director: something, id: 1}
                if (newEntry.containsKey(field)) {
                    // pass arg that contains the tableName to create unique keys
                    newEntry.put(arg, value);
                } else {
                    newEntry.put(field, value);
                }

            }
        }
    }



    public List<LinkedHashMap<String, String>> getSelectWithJoin
            (List<LinkedHashMap<String, String>> table1, List<LinkedHashMap<String, String>> table2, String
                    field1, String field2) {
        List<LinkedHashMap<String, String>> resultsTable = new ArrayList<>();
        LinkedHashMap<String, String> newEntry = new LinkedHashMap<>();
        String[] SELECTArgs = (String[]) this.queriesMap.get("SELECT");

        for (LinkedHashMap<String, String> entry1 : table1) {
            for (LinkedHashMap<String, String> entry2 : table2) {
                if (entry2.get(field2).equals(entry1.get(field1))) {   // (entry1[field1] == entry2[field2])
                    // if (where)
                    doSELECT(entry1, firstTableName, SELECTArgs, newEntry);
                    doSELECT(entry2, secondTableName, SELECTArgs, newEntry);
                    resultsTable.add(newEntry);
                }
            }
            newEntry = new LinkedHashMap<>();
        }

        return resultsTable;
    }



/*

    function handleWhere(statemen1, operator, statement2) {
            switch(operator):
                "="
                return statement1 == statement2;
                ">"
                return statement1 > statement2;
                "<"
                return statement1 < statement2;
     */
}
