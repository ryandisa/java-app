/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package scheduledexportcsv;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 *
 * @author Refly IDFA
 */
public class CSVUtil {

    private String filepath;
    private List data;
    private ResultSet resultSet;

    public CSVUtil(String filepath) {
        this.filepath = filepath;
    }

    public CSVUtil(String filepath, List data) {
        this.filepath = filepath;
        this.data = data;
    }

    public CSVUtil(String filepath, ResultSet resultSet) {
        this.filepath = filepath;
        this.resultSet = resultSet;
    }

    public String getFilepath() {
        return filepath;
    }

    public void setFilepath(String filepath) {
        this.filepath = filepath;
    }

    public List getData() {
        return data;
    }

    public void setData(List data) {
        this.data = data;
    }

    public List readData() throws FileNotFoundException, IOException {
        CSVReader csvReader = new CSVReader(new FileReader(filepath));
        List list = csvReader.readAll();
        csvReader.close();
        return list;
    }

    public void writeResultSet() throws IOException, SQLException {
        CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(new FileOutputStream(filepath), "UTF-8"));

        int col = resultSet.getMetaData().getColumnCount();
        int row = 0;
        String[] resultrow = new String[col];

        while (resultSet.next()) {
            if (row == 0) {
                for (int i = 0; i < col; i++) {
                    resultrow[i] = resultSet.getMetaData().getColumnLabel(i + 1);
                }
                csvWriter.writeNext(resultrow, false);
            }
            for (int i = 0; i < col; i++) {
                resultrow[i] = resultSet.getObject(i + 1) == null ? "NULL" : resultSet.getObject(i + 1).toString();
            }
            csvWriter.writeNext(resultrow, false);
            row++;
        }
        csvWriter.close();
    }
}
