/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package taxinvoiceautomationsystem;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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

    public void writeData() throws IOException {
        CSVWriter csvWriter = new CSVWriter(new FileWriter(filepath));
        csvWriter.writeAll(data);
        csvWriter.close();
    }

    public void writeResultSet() throws IOException, SQLException {
        CSVWriter csvWriter = new CSVWriter(new FileWriter(filepath));
        csvWriter.writeAll(resultSet, true);
        csvWriter.close();
    }
}
