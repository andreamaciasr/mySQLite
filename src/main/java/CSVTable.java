import com.opencsv.CSVReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class CSVTable {
    private final List<LinkedHashMap<String, Object>> table;
    private String[] headers;

    public CSVTable() {
        this.table = new ArrayList<>();
    }

    public void createTableFromCSV(String filePath) {
        int id = 0;
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            headers = reader.readNext();
            String[] line;
            while ((line = reader.readNext()) != null) {
                LinkedHashMap<String, Object> row = new LinkedHashMap<>();
                row.put("InternalId", String.valueOf(id));
                id++;
                for (int i = 0; i < headers.length; i++) {
                    row.put(headers[i], line[i]);
                }
                table.add(row);
            }
        } catch (IOException | com.opencsv.exceptions.CsvValidationException e) {
            e.printStackTrace();
        }
    }

    public String[] getColumnsNames() {
        return headers != null ? headers : new String[0];
    }

    public List<LinkedHashMap<String, Object>> getTable() {
        return table;
    }
}