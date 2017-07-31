/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package scheduledexportcsv;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;

/**
 *
 * @author Refly IDFA
 */
public class SchedulerDAO extends TimerTask implements StatusProperties{

    private Date extractStart, extractEnd;
    private String filepathQuery, filepathExport, filenameExport;
    private String username, password;
    private String message;
    private int status;
    private PropertyChangeSupport changes;

    public SchedulerDAO() {
        this.changes = new PropertyChangeSupport(this);
    }

    public SchedulerDAO(Date extractStart, Date extractEnd, String filepathQuery, String username, String password, String filepathExport, String filenameExport) {
        this.extractStart = extractStart;
        this.extractEnd = extractEnd;
        this.filepathQuery = filepathQuery;
        this.username = username;
        this.password = password;
        this.filepathExport = filepathExport;
        this.filenameExport = filenameExport;
        this.changes = new PropertyChangeSupport(this);
    }

    public void executeProcess() {
        try {
            propertyStatusChange(STAT_START);
            message = "";
            extractData();
            propertyStatusChange(STAT_SUCCESS);
            propertyMessageChange("Completed!");
        } catch (Exception e) {
            System.out.println(e);
            propertyStatusChange(STAT_DB_ERROR);
            propertyMessageChange("DATA EXTRACTION ERROR\n" + e);
        } finally {
            propertyStatusChange(STAT_END);
            message = "";
        }
    }

    public void extractData() throws SQLException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        propertyStatusChange(STAT_CONNECT_ANONDB);
        propertyMessageChange("Connecting database...");
        ConnectionManager cm = new ConnectionManager();
        Connection conn = cm.open(username, password);
        
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);

        String sql = readQuery();

        sql = sql.replaceAll("@extractstart", "\'" + dateFormat(extractStart) + "\'");
        sql = sql.replaceAll("@extractend", "\'" + dateFormat(extractEnd) + "\'");
        System.out.println(sql);

        propertyStatusChange(STAT_EXECUTE_QUERY);
        propertyMessageChange("Fetching data");
        ResultSet rs = stmt.executeQuery(sql);

        propertyStatusChange(STAT_WRITE_DATA);
        propertyMessageChange("Writing " + filenameExport);
        new CSVUtil(filepathExport + "\\" + filenameExport + ".csv", rs).writeResultSet();
        
        conn.close();
    }

    private String readQuery() throws FileNotFoundException, IOException {
        String query = "";

        BufferedReader bufferedReader = new BufferedReader(new FileReader(filepathQuery));
        StringBuilder sb = new StringBuilder();
        String line;

        while ((line = bufferedReader.readLine()) != null) {
            sb.append(line + "\n");
        }
        
        bufferedReader.close();
        query = sb.toString();

        return query;
    }

    private String dateFormat(Date date) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(date);
    }

    private void propertyStatusChange(int newStatus) {
        changes.firePropertyChange(SCHEDULER_STATUS, status, newStatus);
        status = newStatus;
    }

    private void propertyMessageChange(String message) {
        changes.firePropertyChange(SCHEDULER_MESSAGE, this.message, message);
        this.message = message;
    }

    public void addPropertyChangeListener(PropertyChangeListener listener) {
        changes.addPropertyChangeListener(listener);
    }

    @Override
    public void run() {
        executeProcess();
    }
}
