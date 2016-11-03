/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package scheduledexportcsv;

import com.opencsv.CSVWriter;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Refly IDFA
 */
public class SchedulerDAO extends TimerTask {

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

    private Date extractStart, extractEnd;
    private String outFile;
    private PropertyChangeSupport changes;
    private int status;

    public SchedulerDAO(Date extractStart, Date extractEnd, String outFile) {
        this.extractStart = extractStart;
        this.extractEnd = extractEnd;
        this.outFile = outFile;
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

    public String getOutFile() {
        return outFile;
    }

    public void setOutFile(String outFile) {
        this.outFile = outFile;
    }

    public PropertyChangeSupport getChanges() {
        return changes;
    }

    public void setChanges(PropertyChangeSupport changes) {
        this.changes = changes;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public void addPropertyChangeListener(PropertyChangeListener listener) {
        changes.addPropertyChangeListener(listener);
    }

    public void extractData() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String start = dateFormat.format(extractStart);
        String end = dateFormat.format(extractEnd);

        changes.firePropertyChange(PROPERTY_STATUS, status, STAT_CONNECT_DB);
        status = STAT_CONNECT_DB;

        ConnectionManager cm = new ConnectionManager();
        Connection conn = cm.open();

        System.out.println("Executing query...");
        changes.firePropertyChange(PROPERTY_STATUS, status, STAT_EXECUTE_QUERY);
        status = STAT_EXECUTE_QUERY;

        Statement stmt;
        ResultSet rs;

        try {
            String sql = "SELECT \n"
                    + "    MAX(SAP_RTS_PO_Number) AS sap_rts_po_number,\n"
                    + "    supplier,\n"
                    + "    name_en,\n"
                    + "    uid,\n"
                    + "    erp_reference,\n"
                    + "    po_type,\n"
                    + "    MAX(TransDate) AS rts_date,\n"
                    + "    cost\n"
                    + "FROM\n"
                    + "    (SELECT \n"
                    + "        CONCAT('IDR', RIGHT(CONCAT('0000', id_supplier_return_package), 7)) AS SAP_RTS_PO_Number,\n"
                    + "            package_number AS RTS_PO_Number,\n"
                    + "            ReverseRTS.*,\n"
                    + "            CONCAT('ID', s.bob_id_supplier) AS Supplier,\n"
                    + "            s.name_en\n"
                    + "    FROM\n"
                    + "        oms_live.oms_supplier_return_package rtsp\n"
                    + "    JOIN oms_live.oms_supplier_return_package_item rtspi ON rtsp.id_supplier_return_package = rtspi.fk_supplier_return_package\n"
                    + "        AND fk_supplier_return_package_item_status = 2\n"
                    + "        AND fk_supplier_return_package_status = 2\n"
                    + "    JOIN oms_live.oms_supplier_return_picklist_item rtspicki ON rtspi.fk_supplier_return_picklist_item = rtspicki.id_supplier_return_picklist_item\n"
                    + "    JOIN oms_live.oms_supplier_return_picklist rtspick ON rtspicki.fk_supplier_return_picklist = rtspick.id_supplier_return_picklist\n"
                    + "    JOIN oms_live.wms_inventory i ON rtspicki.fk_inventory = i.id_inventory\n"
                    + "    JOIN oms_live.oms_supplier_return rts ON rtspick.fk_supplier_return = rts.id_supplier_return\n"
                    + "    JOIN oms_live.ims_supplier s ON rts.fk_supplier = s.id_supplier\n"
                    + "    JOIN (SELECT DISTINCT\n"
                    + "        'RTS',\n"
                    + "            PEWO.po_type,\n"
                    + "            PEWO.UID,\n"
                    + "            p.SKU,\n"
                    + "            p.erp_reference,\n"
                    + "            PEWO.cost,\n"
                    + "            PEWO.id_inventory,\n"
                    + "            TransDate\n"
                    + "    FROM\n"
                    + "        (SELECT DISTINCT\n"
                    + "        id_inventory\n"
                    + "    FROM\n"
                    + "        oms_live.wms_inventory_history ih\n"
                    + "    JOIN (SELECT \n"
                    + "        UID, MAX(id_inventory_history) AS id_inventory_history\n"
                    + "    FROM\n"
                    + "        oms_live.wms_inventory_history\n"
                    + "    WHERE\n"
                    + "        history_created_at < '" + start + "'\n"
                    + "    GROUP BY UID) PS ON ih.id_inventory_history = PS.id_inventory_history\n"
                    + "        AND ih.fk_inventory_status <> 18) PSWO\n"
                    + "    RIGHT JOIN (SELECT \n"
                    + "        id_inventory,\n"
                    + "            ih.created_at,\n"
                    + "            ih.uid,\n"
                    + "            cost,\n"
                    + "            po_type,\n"
                    + "            fk_product,\n"
                    + "            MAX(history_created_at) AS TransDate,\n"
                    + "            fk_delivery_receipt_item\n"
                    + "    FROM\n"
                    + "        oms_live.wms_inventory_history ih\n"
                    + "    JOIN (SELECT \n"
                    + "        UID, MAX(id_inventory_history) AS id_inventory_history\n"
                    + "    FROM\n"
                    + "        oms_live.wms_inventory_history\n"
                    + "    WHERE\n"
                    + "        history_created_at < '" + end + "'\n"
                    + "    GROUP BY UID) PE ON ih.id_inventory_history = PE.id_inventory_history\n"
                    + "        AND ih.fk_inventory_status = 18\n"
                    + "    WHERE\n"
                    + "        ih.history_created_at > '" + start + "'\n"
                    + "    GROUP BY id_inventory , ih.uid , cost , po_type , fk_product) PEWO ON PSWO.id_inventory = PEWO.id_inventory\n"
                    + "    JOIN oms_live.ims_product p ON PEWO.fk_product = p.id_product\n"
                    + "    JOIN oms_live.wms_delivery_receipt_item dri ON PEWO.fk_delivery_receipt_item = dri.id_delivery_receipt_item\n"
                    + "    JOIN oms_live.wms_delivery_receipt dr ON dri.fk_delivery_receipt = dr.id_delivery_receipt\n"
                    + "    WHERE\n"
                    + "        (dr.created_at > '" + start + "'\n"
                    + "            OR PSWO.id_inventory IS NOT NULL)) ReverseRTS ON ReverseRTS.id_inventory = i.id_inventory) x\n"
                    + "GROUP BY Supplier , uid , erp_reference , po_type";

            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);
            rs = stmt.executeQuery(sql);

            changes.firePropertyChange(PROPERTY_STATUS, status, STAT_FETCH_RESULT);
            status = STAT_FETCH_RESULT;

            int filenum = 1;
            int row = 0;
            int col = rs.getMetaData().getColumnCount();

            start = start.replaceAll("-", "");
            end = end.replaceAll("-", "");

            CSVWriter writer = new CSVWriter(new FileWriter(outFile + "\\" + "RTS_"
                    + start + "_" + end + "_" + filenum + ".csv"));

            String[] resultrow = new String[col];

            while (rs.next()) {
                if (row == 1000000) {
                    filenum++;
                    row = 0;
                    writer.close();
                    writer = new CSVWriter(new FileWriter(outFile + "\\" + "RTS_"
                            + start + "_" + end + "_" + filenum + ".csv"));
                }
                if (row == 0) {
                    for (int i = 0; i < col; i++) {
                        resultrow[i] = rs.getMetaData().getColumnLabel(i + 1);
                    }
                    writer.writeNext(resultrow, false);
                }
                for (int i = 0; i < col; i++) {
                    resultrow[i] = rs.getObject(i + 1) == null ? "NULL" : rs.getObject(i + 1).toString();
                }
                writer.writeNext(resultrow, false);
                row++;
            }

            writer.close();

            changes.firePropertyChange(PROPERTY_STATUS, status, STAT_DISCONNECT_DB);
            status = STAT_DISCONNECT_DB;
            rs.close();
            stmt.close();
            conn.close();

            System.out.println("Done!");
            changes.firePropertyChange(PROPERTY_STATUS, status, STAT_DONE);
            status = STAT_DONE;
            init();
        } catch (Exception ex) {
            System.out.println("Database error!");
            changes.firePropertyChange(PROPERTY_STATUS, status, STAT_DB_ERROR);
            status = STAT_DB_ERROR;

            Logger.getLogger(SchedulerDAO.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            cm.close();
        }
    }

    public void init() {
        changes.firePropertyChange(PROPERTY_STATUS, status, STAT_INIT);
        status = STAT_INIT;
    }

    @Override
    public void run() {
        extractData();
    }
}
