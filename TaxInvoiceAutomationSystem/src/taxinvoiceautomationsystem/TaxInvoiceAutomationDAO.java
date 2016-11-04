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

    public static final String FILENAME_TRANSACTION = "\\transactions.csv";
    public static final String FILENAME_ADJUSTMENT = "\\adjustments.csv";

    private Date extractStart, extractEnd;
    private String filepathImport, filepathExport;
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
            extractTransactions();
            importData();
            calculateResult();
            writeResult();
        } catch (SQLException | IOException e) {
        }
    }

    public void extractTransactions() throws SQLException, IOException {
        ConnectionManagerAnonDB cm = new ConnectionManagerAnonDB();
        Connection conn = cm.open();
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);

        String sql = "SELECT \n"
                + "    *\n"
                + "FROM\n"
                + "    (SELECT \n"
                + "        tr.number 'transaction_number',\n"
                + "            tt.description 'transaction_type',\n"
                + "            tr.value,\n"
                + "            tr.created_at 'transaction_date',\n"
                + "            tr.description,\n"
                + "            soi.id_sales_order_item 'sap_item_id',\n"
                + "            soish.created_at 'delivered_date',\n"
                + "            sel.src_id 'bob_id_supplier',\n"
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
                + "        screport.transaction tr\n"
                + "    LEFT JOIN screport.transaction_type tt ON tr.fk_transaction_type = tt.id_transaction_type\n"
                + "    LEFT JOIN screport.seller sel ON tr.fk_seller = sel.id_seller\n"
                + "    LEFT JOIN screport.sales_order_item scsoi ON tr.ref = scsoi.id_sales_order_item\n"
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
                + "        tr.created_at >= '" + dateFormat(extractEnd) + "'\n"
                + "            AND tr.created_at < '" + dateFormat(extractEnd) + "'\n"
                + "            AND tr.fk_transaction_type IN (16 , 3, 15)\n"
                + "    GROUP BY transaction_number) sc;";

        ResultSet rs = stmt.executeQuery(sql);
        new CSVUtil(filepathImport + FILENAME_TRANSACTION, rs).writeResultSet();
        List<String[]> transactionID = new CSVUtil(filepathImport + FILENAME_ADJUSTMENT).readData();
        sql = "";

        for (String[] id : transactionID) {
            sql = "'" + id[0] + "',";
        }

        sql = sql.substring(0, sql.length() - 2);

        sql = "SELECT \n"
                + "    *\n"
                + "FROM\n"
                + "    (SELECT \n"
                + "        tr.number 'transaction_number',\n"
                + "            tt.description 'transaction_type',\n"
                + "            tr.value,\n"
                + "            tr.created_at 'transaction_date',\n"
                + "            tr.description,\n"
                + "            soi.id_sales_order_item 'sap_item_id',\n"
                + "            soish.created_at 'delivered_date',\n"
                + "            sel.src_id 'bob_id_supplier',\n"
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
                + "        screport.transaction tr\n"
                + "    LEFT JOIN screport.transaction_type tt ON tr.fk_transaction_type = tt.id_transaction_type\n"
                + "    LEFT JOIN screport.seller sel ON tr.fk_seller = sel.id_seller\n"
                + "    LEFT JOIN screport.sales_order_item scsoi ON tr.ref = scsoi.id_sales_order_item\n"
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

        rs = stmt.executeQuery(sql);
        new CSVUtil(filepathImport + FILENAME_ADJUSTMENT, rs).writeResultSet();
    }

    public void importData() throws SQLException {
        ConnectionManagerLocalhost cm = new ConnectionManagerLocalhost();
        Connection conn = cm.open();
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);

        int i = stmt.executeUpdate("TRUNCATE TABLE tias.transaction;");
        i = stmt.executeUpdate("LOAD DATA LOCAL INFILE\n"
                + "	'" + filepathImport + FILENAME_TRANSACTION + "'\n"
                + "IGNORE INTO TABLE\n"
                + "	ship_calc_env.anondb_extract\n"
                + "COLUMNS TERMINATED BY\n"
                + "	','\n"
                + "OPTIONALLY ENCLOSED BY\n"
                + "	'\"'\n"
                + "IGNORE 1 ROWS;");
        i = stmt.executeUpdate("LOAD DATA LOCAL INFILE\n"
                + "	'" + filepathImport + FILENAME_ADJUSTMENT + "'\n"
                + "IGNORE INTO TABLE\n"
                + "	ship_calc_env.anondb_extract\n"
                + "COLUMNS TERMINATED BY\n"
                + "	','\n"
                + "OPTIONALLY ENCLOSED BY\n"
                + "	'\"'\n"
                + "IGNORE 1 ROWS;");
    }

    public void calculateResult() {
    }

    public void writeResult() {
    }

    private String dateFormat(Date date) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(date);
    }

    @Override
    public void run() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
