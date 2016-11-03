/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package taxinvoiceautomationsystem;

import java.beans.PropertyChangeSupport;
import java.util.TimerTask;

/**
 *
 * @author Refly IDFA
 */
public class TaxInvoiceAutomationDAO extends TimerTask {

    public static final String PROPERTY_STATUS = "SCHEDULER_STATUS";
    public static final String PROPERTY_PROGRESS = "SCHEDULER_PROGRESS";
    public static final String PROPERTY_RESULT = "SCHEDULER_RESULT";
    public static final int STAT_INIT = 0;
    public static final int STAT_CONNECT_DB = 1;
    public static final int STAT_EXECUTE_QUERY = 2;
    public static final int STAT_FETCH_RESULT = 3;
    public static final int STAT_WRITE_XLSX = 4;
    public static final int STAT_DISCONNECT_DB = 5;
    public static final int STAT_DONE = 6;
    public static final int STAT_DB_ERROR = 990;
    public static final int STAT_WRITE_ERROR = 991;

    private String filepathImport, filepathExport;
    private int status;
    private PropertyChangeSupport changes;

    public TaxInvoiceAutomationDAO(String filepathImport, String filepathExport) {
        this.filepathImport = filepathImport;
        this.filepathExport = filepathExport;
        this.changes = new PropertyChangeSupport(this);
    }

    public String getFilepathImport() {
        return filepathImport;
    }

    public void setFilepathImport(String filepathImport) {
        this.filepathImport = filepathImport;
    }

    public String getFilepathExport() {
        return filepathExport;
    }

    public void setFilepathExport(String filepathExport) {
        this.filepathExport = filepathExport;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public PropertyChangeSupport getChanges() {
        return changes;
    }

    public void setChanges(PropertyChangeSupport changes) {
        this.changes = changes;
    }

    @Override
    public void run() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
