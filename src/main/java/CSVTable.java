import com.opencsv.CSVReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class CSVTable {


    public List<LinkedHashMap<String, String>> createTableFromCSV(String filePath) {
        int id = 0;
        List<LinkedHashMap<String, String>> table = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            String[] headers = reader.readNext();
            String[] line;
            while ((line = reader.readNext()) != null) {
                LinkedHashMap<String, String> row = new LinkedHashMap<>();
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
        return table;
    }
}
