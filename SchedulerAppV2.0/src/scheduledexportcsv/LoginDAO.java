/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package scheduledexportcsv;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.File;
import java.sql.Connection;
import java.util.List;

/**
 *
 * @author refly.maliangkay
 */
public class LoginDAO implements StatusProperties {

    private String username;
    private String password;
    private boolean isRememberme;

    private String filepathExport = new File("").getAbsolutePath();
    private String filenameExport = "db_access";

    private String message;
    private PropertyChangeSupport changes;
    private int status;

    public LoginDAO() {
        this.changes = new PropertyChangeSupport(this);
    }

    public LoginDAO(String username, String password, boolean isRememberme) {
        this.username = username;
        this.password = password;
        this.isRememberme = isRememberme;
        this.changes = new PropertyChangeSupport(this);
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public boolean getIsRememberme() {
        return isRememberme;
    }

    public String getFilenameExport() {
        return filenameExport;
    }

    public String getFilepathExport() {
        return filepathExport;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setIsRememberme(boolean isRememberme) {
        this.isRememberme = isRememberme;
    }

    public void setFilepathExport(String filepathExport) {
        this.filepathExport = filepathExport;
    }

    public void setFilenameExport(String filenameExport) {
        this.filenameExport = filenameExport;
    }

    public void getAccess() {
        try {
            List<String[]> list = new CSVUtil(filepathExport + "\\" + filenameExport + ".csv").readData();

            username = list.get(1)[0];
            password = list.get(1)[1];
        } catch (Exception e) {
            System.out.println("no file selected");
        }
    }

    public void login() {
        try {
            propertyStatusChange(STAT_START);
            message = "";

            if (isRememberme) {
                new CSVUtil(filepathExport + "\\" + filenameExport + ".csv").writeAccess(username, password);
            }

            Connection conn = new ConnectionManager().open(username, password);
            System.out.println("Disconnecting from database...");
            conn.close();

            propertyStatusChange(STAT_SUCCESS);
            propertyMessageChange("Login Success!");
        } catch (Exception e) {
            System.out.println(e);
            propertyStatusChange(STAT_DB_ERROR);
            propertyMessageChange("LOGIN FAILED\n" + e);
        } finally {
            propertyStatusChange(STAT_END);
            message = "";
        }
    }

    private void propertyStatusChange(int newStatus) {
        changes.firePropertyChange(LOGIN_STATUS, status, newStatus);
        status = newStatus;
    }

    private void propertyMessageChange(String message) {
        changes.firePropertyChange(LOGIN_MESSAGE, this.message, message);
        this.message = message;
    }

    public void addPropertyChangeListener(PropertyChangeListener listener) {
        changes.addPropertyChangeListener(listener);
    }

}
