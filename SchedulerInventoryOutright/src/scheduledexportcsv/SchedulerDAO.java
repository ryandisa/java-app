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
                    + "    *\n"
                    + "FROM\n"
                    + "    (SELECT \n"
                    + "        p.sku AS sku,\n"
                    + "            i.uid AS uid,\n"
                    + "            i.barcode AS manufacturer_barcode,\n"
                    + "            p.name AS item_description,\n"
                    + "            IF(l.description IS NULL, '', l.description) AS location,\n"
                    + "            IF(wis.name IS NULL, '', wis.name) AS status,\n"
                    + "            i.created_at AS created_at,\n"
                    + "            i.updated_at AS updated_at,\n"
                    + "            i.expiry_date AS expiry_date,\n"
                    + "            poi.cost AS cogp,\n"
                    + "            i.cost AS write_down_cost,\n"
                    + "            IF(po.po_number IS NULL, '', po.po_number) AS po_number,\n"
                    + "            IF(po.po_type IS NULL, '', po.po_type) AS po_type,\n"
                    + "            IF(so.order_nr IS NULL, '', so.order_nr) AS order_nr,\n"
                    + "            s.name AS supplier_name,\n"
                    + "            i.changed_reason_status,\n"
                    + "            poct.name AS po_contract_type,\n"
                    + "            wh.name AS warehouse_name\n"
                    + "    FROM\n"
                    + "        oms_live.wms_inventory i\n"
                    + "    JOIN oms_live.ims_product p ON i.fk_product = p.id_product\n"
                    + "    LEFT JOIN oms_live.ims_location l ON l.id_location = i.fk_location\n"
                    + "    JOIN oms_live.ims_purchase_order_item poi ON i.fk_purchase_order_item = poi.id_purchase_order_item\n"
                    + "    JOIN oms_live.ims_purchase_order po ON po.id_purchase_order = poi.fk_purchase_order\n"
                    + "    LEFT JOIN oms_live.ims_sales_order_item soi ON poi.fk_sales_order_item = soi.id_sales_order_item\n"
                    + "    LEFT JOIN oms_live.ims_sales_order so ON so.id_sales_order = soi.fk_sales_order\n"
                    + "    LEFT JOIN oms_live.wms_inventory_status wis ON i.fk_inventory_status = wis.id_inventory_status\n"
                    + "    JOIN oms_live.ims_supplier_product sp ON poi.fk_supplier_product = sp.id_supplier_product\n"
                    + "    JOIN oms_live.ims_supplier sup ON sp.fk_supplier = sup.id_supplier\n"
                    + "    JOIN oms_live.ims_purchase_order_contract_type poct ON po.fk_purchase_order_contract_type = poct.id_purchase_order_contract_type\n"
                    + "    JOIN oms_live.oms_warehouse AS wh ON wh.id_warehouse = i.fk_current_warehouse\n"
                    + "    LEFT JOIN bob_live.supplier s ON sup.bob_id_supplier = s.id_supplier\n"
                    + "    WHERE\n"
                    + "        i.fk_inventory_status NOT IN (6 , 15, 28)\n"
                    + "            AND poct.name = 'Outright') result;";

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

            CSVWriter writer = new CSVWriter(new FileWriter(outFile + "\\"
                    + "INVENTORY_OUTRIGHT" + "_" + filenum + ".csv"));

            String[] resultrow = new String[col];

            while (rs.next()) {
                if (row == 1000000) {
                    filenum++;
                    row = 0;
                    writer.close();
                    writer = new CSVWriter(new FileWriter(outFile + "\\"
                            + "INVENTORY_OUTRIGHT" + "_" + filenum + ".csv"));
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
