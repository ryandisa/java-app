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
public class SchedulerDAO extends TimerTask {

    public static final String PROPERTY_STATUS = "SCHEDULER_STATUS";
    public static final String PROPERTY_MESSAGE = "SCHEDULER_MESSAGE";
    public static final String PROPERTY_PROGRESS = "SCHEDULER_PROGRESS";
    public static final String PROPERTY_RESULT = "SCHEDULER_RESULT";

    public static final int STAT_START = 999900;
    public static final int STAT_SUCCESS = 999990;
    public static final int STAT_END = 999999;

    public static final int STAT_CONNECT_ANONDB = 110;
    public static final int STAT_CONNECT_LOCALDB = 120;
    public static final int STAT_DISCONNECT_DB = 199;

    public static final int STAT_READ_QUERY = 200;

    public static final int STAT_EXECUTE_QUERY = 300;

    public static final int STAT_FETCH_RESULT = 400;

    public static final int STAT_READ_DATA = 500;

    public static final int STAT_WRITE_DATA = 600;

    public static final int STAT_DAO_ERROR = 9000;
    public static final int STAT_DB_ERROR = 9100;
    public static final int STAT_WRITE_ERROR = 9200;
    public static final int STAT_READ_ERROR = 9300;

    private Date extractStart, extractEnd;
    private String filepathQuery, filepathExport, filenameExport;
    private String message;
    private int status;
    private PropertyChangeSupport changes;

    public SchedulerDAO() {
    }

    public SchedulerDAO(Date extractStart, Date extractEnd, String filepathQuery, String filepathExport, String filenameExport) {
        this.extractStart = extractStart;
        this.extractEnd = extractEnd;
        this.filepathQuery = filepathQuery;
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

    public void extractData() throws SQLException, IOException {
        propertyStatusChange(STAT_CONNECT_ANONDB);
        propertyMessageChange("Connecting database...");
        ConnectionManager cm = new ConnectionManager();
        Connection conn = cm.open();
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
    }

    private String readQuery() throws FileNotFoundException, IOException {
        String query = "";

        BufferedReader bufferedReader = new BufferedReader(new FileReader(filepathQuery));
        StringBuilder sb = new StringBuilder();
        String line;

        while ((line = bufferedReader.readLine()) != null) {
            sb.append(line + "\n");
        }

        query = sb.toString();

        return query;
    }

    private String dateFormat(Date date) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(date);
    }

    private void propertyStatusChange(int newStatus) {
        changes.firePropertyChange(PROPERTY_STATUS, status, newStatus);
        status = STAT_CONNECT_ANONDB;
    }

    private void propertyMessageChange(String message) {
        changes.firePropertyChange(PROPERTY_MESSAGE, this.message, message);
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
