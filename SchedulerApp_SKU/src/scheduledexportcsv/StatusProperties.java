/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package scheduledexportcsv;

/**
 *
 * @author refly.maliangkay
 */
public interface StatusProperties {

    public static final String SCHEDULER_STATUS = "SCHEDULER_STATUS";
    public static final String SCHEDULER_MESSAGE = "SCHEDULER_MESSAGE";
    public static final String SCHEDULER_PROGRESS = "SCHEDULER_PROGRESS";
    public static final String SCHEDULER_RESULT = "SCHEDULER_RESULT";
    
    public static final String LOGIN_STATUS = "LOGIN_STATUS";
    public static final String LOGIN_MESSAGE = "LOGIN_MESSAGE";
    public static final String LOGIN_PROGRESS = "LOGIN_PROGRESS";
    public static final String LOGIN_RESULT = "LOGIN_RESULT";

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

}
