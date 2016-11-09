/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package taxinvoiceautomationsystem;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimerTask;

/**
 *
 * @author Refly IDFA
 */
public class TaxInvoiceAutomationDAO extends TimerTask {

    public static final String PROPERTY_STATUS = "SCHEDULER_STATUS";
    public static final String PROPERTY_MESSAGE = "SCHEDULER_MESSAGE";
    public static final String PROPERTY_PROGRESS = "SCHEDULER_PROGRESS";
    public static final String PROPERTY_RESULT = "SCHEDULER_RESULT";

    public static final int STAT_START = 99990;
    public static final int STAT_END = 99999;
    public static final int STAT_CONNECT_ANONDB = 10;
    public static final int STAT_CONNECT_LOCALDB = 11;
    public static final int STAT_DISCONNECT_DB = 12;
    public static final int STAT_EXECUTE_QUERY = 20;
    public static final int STAT_CLEANUP_DATA = 21;
    public static final int STAT_FETCH_RESULT = 30;

    public static final int STAT_READ_TRANSACTION = 40;
    public static final int STAT_READ_ADJUSTMENT = 41;
    public static final int STAT_READ_SELLER_DETAILS = 42;
    public static final int STAT_READ_SELLER_DETAILS_MANUAL = 43;
    public static final int STAT_READ_RESULT = 44;

    public static final int STAT_WRITE_TRANSACTION = 60;
    public static final int STAT_WRITE_ADJUSTMENT = 61;
    public static final int STAT_WRITE_SELLER_DETAILS = 62;
    public static final int STAT_WRITE_RESULT = 63;
    public static final int STAT_DONE = 100;

    public static final int STAT_DAO_ERROR = 9000;
    public static final int STAT_DB_ERROR = 9100;
    public static final int STAT_WRITE_ERROR = 9200;
    public static final int STAT_READ_ERROR = 9300;

    public static final String FILENAME_TRANSACTIONS = "\\transactions.csv";
    public static final String FILENAME_ADJUSTMENTS = "\\adjustments.csv";
    public static final String FILENAME_SELLER_DETAILS = "\\seller_details.csv";
    public static final String FILENAME_SELLER_DETAILS_MANUAL = "\\seller_details_manual.csv";
    public static final String FILENAME_RESULT = "\\result.csv";

    private Date extractStart, extractEnd;
    private String filepathImport, filepathExport;
    private String message;
    private int status;
    private PropertyChangeSupport changes;

    public TaxInvoiceAutomationDAO() {
    }

    public TaxInvoiceAutomationDAO(Date extractStart, Date extractEnd, String filepathImport, String filepathExport) {
        this.extractStart = extractStart;
        this.extractEnd = extractEnd;
        this.filepathImport = filepathImport;
        this.filepathExport = filepathExport;
        this.changes = new PropertyChangeSupport(this);
    }

    public Date getExtractStart() {
        return extractStart;
    }

    public void setExtractStart(Date extractStart) {
        this.extractStart = extractStart;
    }

    public Date getExtractEnd() {
        return extractEnd;
    }

    public void setExtractEnd(Date extractEnd) {
        this.extractEnd = extractEnd;
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

    public void addPropertyChangeListener(PropertyChangeListener listener) {
        changes.addPropertyChangeListener(listener);
    }

    public void executeProcess() {
        try {
            propertyStatusChange(STAT_START);
            message = "";
            extractTransactions();
            importData();
            calculateResult();
            propertyStatusChange(STAT_DONE);
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

    public void extractTransactions() throws SQLException, IOException {
        propertyStatusChange(STAT_CONNECT_ANONDB);
        propertyMessageChange("Connecting database...");
        ConnectionManagerAnonDB cm = new ConnectionManagerAnonDB();
        Connection conn = cm.open();
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);

        String sql = "SELECT \n"
                + "    *\n"
                + "FROM\n"
                + "    (SELECT \n"
                + "        IFNULL(tr.number, 'NULL') 'transaction_number',\n"
                + "            IFNULL(tt.description, 'NULL') 'transaction_type',\n"
                + "            IFNULL(tr.value, 'NULL') 'value',\n"
                + "            IFNULL(DATE_FORMAT(tr.created_at, '%Y-%m-%d %H:%i:%s'), 'NULL') 'transaction_date',\n"
                + "            IFNULL(tr.description, 'NULL') 'description',\n"
                + "            IFNULL(soi.id_sales_order_item, 'NULL') 'sap_item_id',\n"
                + "            IFNULL(DATE_FORMAT(soish.created_at, '%Y-%m-%d %H:%i:%s'), 'NULL') 'delivered_date',\n"
                + "            IFNULL(sel.src_id, 'NULL') 'bob_id_supplier'"
                + "    FROM\n"
                + "        screport.transaction tr\n"
                + "    LEFT JOIN screport.transaction_type tt ON tr.fk_transaction_type = tt.id_transaction_type\n"
                + "    LEFT JOIN screport.sales_order_item scsoi ON tr.ref = scsoi.id_sales_order_item\n"
                + "    LEFT JOIN screport.seller sel ON tr.fk_seller = sel.id_seller\n"
                + "    LEFT JOIN oms_live.ims_sales_order_item soi ON scsoi.src_id = soi.id_sales_order_item\n"
                + "    LEFT JOIN oms_live.ims_sales_order_item_status_history soish ON soi.id_sales_order_item = soish.fk_sales_order_item\n"
                + "        AND soish.id_sales_order_item_status_history = (SELECT \n"
                + "            MIN(id_sales_order_item_status_history)\n"
                + "        FROM\n"
                + "            oms_live.ims_sales_order_item_status_history\n"
                + "        WHERE\n"
                + "            fk_sales_order_item = soi.id_sales_order_item\n"
                + "                AND fk_sales_order_item_status = 27)\n"
                + "    WHERE\n"
                + "        tr.created_at >= '" + dateFormat(extractStart) + "'\n"
                + "            AND tr.created_at < '" + dateFormat(extractEnd) + "'\n"
                + "            AND tr.fk_transaction_type IN (16 , 3, 15)\n"
                + "    GROUP BY transaction_number) sc;";

        propertyStatusChange(STAT_EXECUTE_QUERY);
        propertyMessageChange("Fetching data");
        ResultSet rs = stmt.executeQuery(sql);

        propertyStatusChange(STAT_WRITE_TRANSACTION);
        propertyMessageChange("Writing " + FILENAME_TRANSACTIONS.replace("\\", ""));
        new CSVUtil(filepathImport + FILENAME_TRANSACTIONS, rs).writeResultSet();

        List<String[]> transactionID = new CSVUtil(filepathImport + FILENAME_ADJUSTMENTS).readData();
        sql = "";
        for (String[] id : transactionID) {
            sql = sql + "'" + id[0] + "',";
        }
        sql = sql.substring(0, sql.length() - 1);

        sql = "SELECT \n"
                + "    *\n"
                + "FROM\n"
                + "    (SELECT \n"
                + "        IFNULL(tr.number, 'NULL') 'transaction_number',\n"
                + "            IFNULL(tt.description, 'NULL') 'transaction_type',\n"
                + "            IFNULL(tr.value, 'NULL') 'value',\n"
                + "            IFNULL(DATE_FORMAT(tr.created_at, '%Y-%m-%d %H:%i:%s'), 'NULL') 'transaction_date',\n"
                + "            IFNULL(tr.description, 'NULL') 'description',\n"
                + "            IFNULL(soi.id_sales_order_item, 'NULL') 'sap_item_id',\n"
                + "            IFNULL(DATE_FORMAT(soish.created_at, '%Y-%m-%d %H:%i:%s'), 'NULL') 'delivered_date',\n"
                + "            IFNULL(sel.src_id, 'NULL') 'bob_id_supplier'"
                + "    FROM\n"
                + "        screport.transaction tr\n"
                + "    LEFT JOIN screport.transaction_type tt ON tr.fk_transaction_type = tt.id_transaction_type\n"
                + "    LEFT JOIN screport.sales_order_item scsoi ON tr.ref = scsoi.id_sales_order_item\n"
                + "    LEFT JOIN screport.seller sel ON tr.fk_seller = sel.id_seller\n"
                + "    LEFT JOIN oms_live.ims_sales_order_item soi ON scsoi.src_id = soi.id_sales_order_item\n"
                + "    LEFT JOIN oms_live.ims_sales_order_item_status_history soish ON soi.id_sales_order_item = soish.fk_sales_order_item\n"
                + "        AND soish.id_sales_order_item_status_history = (SELECT \n"
                + "            MIN(id_sales_order_item_status_history)\n"
                + "        FROM\n"
                + "            oms_live.ims_sales_order_item_status_history\n"
                + "        WHERE\n"
                + "            fk_sales_order_item = soi.id_sales_order_item\n"
                + "                AND fk_sales_order_item_status = 27)\n"
                + "    WHERE\n"
                + "        tr.number IN (" + sql + ")\n"
                + "    GROUP BY transaction_number) sc;";

        propertyStatusChange(STAT_EXECUTE_QUERY);
        propertyMessageChange("Fetching data");
        rs = stmt.executeQuery(sql);

        propertyStatusChange(STAT_WRITE_ADJUSTMENT);
        propertyMessageChange("Writing " + FILENAME_ADJUSTMENTS.replace("\\", ""));
        new CSVUtil(filepathImport + FILENAME_ADJUSTMENTS, rs).writeResultSet();

        propertyStatusChange(STAT_DISCONNECT_DB);
        propertyMessageChange("Disconnecting database...");
        stmt.close();
        conn.close();
        cm.close();
    }

    public void importData() throws SQLException, IOException {
        propertyStatusChange(STAT_CONNECT_LOCALDB);
        propertyMessageChange("Connecting local database...");
        ConnectionManagerLocalhost cm = new ConnectionManagerLocalhost();
        Connection conn = cm.open();
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);

        propertyStatusChange(STAT_CLEANUP_DATA);
        propertyMessageChange("Cleanup local database");
        int i = stmt.executeUpdate("TRUNCATE TABLE tias_java.transaction;");
        i = stmt.executeUpdate("TRUNCATE TABLE tias_java.seller_details;");
        i = stmt.executeUpdate("TRUNCATE TABLE tias_java.seller_details_manual;");
        i = stmt.executeUpdate("TRUNCATE TABLE tias_java.transaction_type;");

        propertyStatusChange(STAT_READ_TRANSACTION);
        propertyMessageChange("Importing " + FILENAME_TRANSACTIONS.replace("\\", ""));
        String filepath = filepathImport + FILENAME_TRANSACTIONS;
        filepath = filepath.replace("\\", "\\\\");
        i = stmt.executeUpdate("LOAD DATA LOCAL INFILE\n"
                + "	'" + filepath + "'\n"
                + "IGNORE INTO TABLE\n"
                + "	tias_java.transaction\n"
                + "COLUMNS TERMINATED BY\n"
                + "	','\n"
                + "OPTIONALLY ENCLOSED BY\n"
                + "	'\"'\n"
                + "IGNORE 1 ROWS;");

        propertyStatusChange(STAT_READ_ADJUSTMENT);
        propertyMessageChange("Importing " + FILENAME_ADJUSTMENTS.replace("\\", ""));
        filepath = filepathImport + FILENAME_ADJUSTMENTS;
        filepath = filepath.replace("\\", "\\\\");
        i = stmt.executeUpdate("LOAD DATA LOCAL INFILE\n"
                + "	'" + filepath + "'\n"
                + "IGNORE INTO TABLE\n"
                + "	tias_java.transaction\n"
                + "COLUMNS TERMINATED BY\n"
                + "	','\n"
                + "OPTIONALLY ENCLOSED BY\n"
                + "	'\"'\n"
                + "IGNORE 1 ROWS;");

        String sql = "SELECT \n"
                + "    bob_id_supplier\n"
                + "FROM\n"
                + "    tias_java.transaction\n"
                + "GROUP BY bob_id_supplier;";
        propertyStatusChange(STAT_EXECUTE_QUERY);
        propertyMessageChange("Fetching seller IDs");
        ResultSet rs = stmt.executeQuery(sql);

        sql = "";
        while (rs.next()) {
            sql = sql + rs.getInt("bob_id_supplier") + ",";
        }
        sql = sql.substring(0, sql.length() - 1);
        sql = "SELECT \n"
                + "    *\n"
                + "FROM\n"
                + "    (SELECT \n"
                + "        sel.src_id 'bob_id_supplier',\n"
                + "            sel.id_seller 'sc_id_seller',\n"
                + "            sel.short_code 'sc_short_code',\n"
                + "            sel.name_company 'legal_name',\n"
                + "            sel.name 'seller_name',\n"
                + "            sel.tax_class,\n"
                + "            sel.vat_number,\n"
                + "            CONCAT(SUBSTRING_INDEX(SUBSTRING_INDEX(sel.tmp_data, 'customercare_address1\":\"', - 1), '\",\"', 1), ', ', SUBSTRING_INDEX(SUBSTRING_INDEX(sel.tmp_data, 'customercare_city\":\"', - 1), '\",\"', 1), ' ', SUBSTRING_INDEX(SUBSTRING_INDEX(sel.tmp_data, 'customercare_postcode\":\"', - 1), '\",\"', 1)) 'address',\n"
                + "            sel.email,\n"
                + "            sel.updated_at\n"
                + "    FROM\n"
                + "        screport.seller sel\n"
                + "    WHERE\n"
                + "        sel.src_id IN (" + sql + ")\n"
                + "    GROUP BY sel.src_id) sc;";

        propertyStatusChange(STAT_CONNECT_ANONDB);
        propertyMessageChange("Connecting database...");
        ConnectionManagerAnonDB cma = new ConnectionManagerAnonDB();
        Connection conna = cma.open();
        Statement stmta = conna.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stmta.setFetchSize(Integer.MIN_VALUE);

        propertyStatusChange(STAT_EXECUTE_QUERY);
        propertyMessageChange("Fetching seller details");
        ResultSet rsa = stmta.executeQuery(sql);

        propertyStatusChange(STAT_WRITE_SELLER_DETAILS);
        propertyMessageChange("Writing " + FILENAME_SELLER_DETAILS.replace("\\", ""));
        new CSVUtil(filepathImport + FILENAME_SELLER_DETAILS, rsa).writeResultSet();

        stmta.close();
        conna.close();
        cma.close();

        propertyStatusChange(STAT_READ_SELLER_DETAILS);
        propertyMessageChange("Importing " + FILENAME_SELLER_DETAILS.replace("\\", ""));
        filepath = filepathImport + FILENAME_SELLER_DETAILS;
        filepath = filepath.replace("\\", "\\\\");
        i = stmt.executeUpdate("LOAD DATA LOCAL INFILE\n"
                + "	'" + filepath + "'\n"
                + "IGNORE INTO TABLE\n"
                + "	tias_java.seller_details\n"
                + "COLUMNS TERMINATED BY\n"
                + "	','\n"
                + "OPTIONALLY ENCLOSED BY\n"
                + "	'\"'\n"
                + "IGNORE 1 ROWS;");

        propertyStatusChange(STAT_READ_SELLER_DETAILS_MANUAL);
        propertyMessageChange("Importing " + FILENAME_SELLER_DETAILS_MANUAL.replace("\\", ""));
        filepath = filepathImport + FILENAME_SELLER_DETAILS_MANUAL;
        filepath = filepath.replace("\\", "\\\\");
        i = stmt.executeUpdate("LOAD DATA LOCAL INFILE\n"
                + "	'" + filepath + "'\n"
                + "IGNORE INTO TABLE\n"
                + "	tias_java.seller_details_manual\n"
                + "COLUMNS TERMINATED BY\n"
                + "	','\n"
                + "OPTIONALLY ENCLOSED BY\n"
                + "	'\"'\n"
                + "IGNORE 1 ROWS;");

        propertyStatusChange(STAT_DISCONNECT_DB);
        propertyMessageChange("Disconnecting database...");
        stmt.close();
        conn.close();
        cm.close();
    }

    public void calculateResult() throws SQLException, IOException {
        propertyStatusChange(STAT_CONNECT_LOCALDB);
        propertyMessageChange("Connecting local database...");
        ConnectionManagerLocalhost cm = new ConnectionManagerLocalhost();
        Connection conn = cm.open();
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);

        String sql = "SELECT \n"
                + "    *\n"
                + "FROM\n"
                + "    (SELECT \n"
                + "        IFNULL(sd.legal_name, IFNULL(sdm.legal_name, '')) 'legal_name',\n"
                + "            IFNULL(sd.seller_name, IFNULL(sdm.seller_name, '')) 'seller_name',\n"
                + "            IFNULL(IF(TRIM(sd.vat_number) = 'null'\n"
                + "                OR TRIM(sd.vat_number) = '', '00.000.000.0-000.000', TRIM(sd.vat_number)), IFNULL(sdm.vat_number, '00.000.000.0-000.000')) 'vat_number1',\n"
                + "            IFNULL(sd.address, IFNULL(sdm.address, '')) 'address',\n"
                + "            IFNULL(sd.email, IFNULL(sdm.email, '')) 'email',\n"
                + "            result.*"
                + "    FROM\n"
                + "        (SELECT \n"
                + "        SUM(IF(tr.transaction_type = 'Payment Fee', value, 0)) 'payment_fee',\n"
                + "            SUM(IF(tr.transaction_type = 'Commission Credit', value, 0)) 'commission_credit',\n"
                + "            SUM(IF(tr.transaction_type = 'Commission', value, 0)) 'commission',\n"
                + "            SUM(IF(tr.transaction_type = 'Seller Credit', value, 0)) 'seller_credit',\n"
                + "            SUM(IF(tr.transaction_type = 'Seller Credit Item', value, 0)) 'seller_credit_item',\n"
                + "            SUM(IF(tr.transaction_type = 'Seller Debit Item', value, 0)) 'seller_debit_item',\n"
                + "            SUM(IF(tr.transaction_type = 'Other Fee', value, 0)) 'other_fee',\n"
                + "            - SUM(tr.value) 'amount_paid_to_seller',\n"
                + "            - SUM(tr.value) / 1.1 'amount_subjected_to_tax',\n"
                + "            - SUM(tr.value) + (SUM(value) / 1.1) 'tax_amount',\n"
                + "            tr.bob_id_supplier\n"
                + "    FROM\n"
                + "        tias_java.transaction tr\n"
                + "    WHERE\n"
                + "        (delivered_date >= '" + dateFormat(extractStart) + "'\n"
                + "            AND delivered_date < '" + dateFormat(extractEnd) + "')\n"
                + "            OR delivered_date IS NULL\n"
                + "            OR delivered_date = '0000-00-00'"
                + "    GROUP BY bob_id_supplier) result\n"
                + "    LEFT JOIN tias_java.seller_details sd ON result.bob_id_supplier = sd.bob_id_supplier\n"
                + "    LEFT JOIN tias_java.seller_details_manual sdm ON result.bob_id_supplier = sdm.bob_id_supplier) result";

        propertyStatusChange(STAT_EXECUTE_QUERY);
        propertyMessageChange("Fetching data");
        ResultSet rs = stmt.executeQuery(sql);

        propertyStatusChange(STAT_WRITE_RESULT);
        propertyMessageChange("Writing " + FILENAME_RESULT.replace("\\", ""));
        new CSVUtil(filepathExport + FILENAME_RESULT, rs).writeResultSet();

        stmt.close();
        conn.close();
        cm.close();
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

    @Override
    public void run() {
        executeProcess();
    }
}
