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
                    + "        *,\n"
                    + "            CASE\n"
                    + "                WHEN last_shipment_provider LIKE '%JNE%' THEN IF(formula_weight < 1, 1, IF(MOD(formula_weight, 1) <= 0.3, FLOOR(formula_weight), CEIL(formula_weight)))\n"
                    + "                ELSE CEIL(formula_weight)\n"
                    + "            END 'rounded_weight',\n"
                    + "            0 AS 'bulky'\n"
                    + "    FROM\n"
                    + "        (SELECT \n"
                    + "        *,\n"
                    + "            length * width * height / 6000 'volumetric_weight',\n"
                    + "            IF(weight > (length * width * height / 6000), weight, (length * width * height / 6000)) 'formula_weight'\n"
                    + "    FROM\n"
                    + "        (SELECT \n"
                    + "        soi.bob_id_sales_order_item,\n"
                    + "            soi.sc_sales_order_item,\n"
                    + "            so.order_nr,\n"
                    + "            so.payment_method,\n"
                    + "            soi.sku,\n"
                    + "            cc.primary_category,\n"
                    + "            sup.id_supplier 'bob_id_supplier',\n"
                    + "            sel.short_code,\n"
                    + "            sup.name 'seller_name',\n"
                    + "            sup.type 'seller_type',\n"
                    + "            sel.tax_class,\n"
                    + "            soi.unit_price,\n"
                    + "            soi.paid_price,\n"
                    + "            soi.shipping_amount,\n"
                    + "            soi.shipping_surcharge,\n"
                    + "            soi.marketplace_commission_fee,\n"
                    + "            soi.coupon_money_value,\n"
                    + "            soi.cart_rule_discount,\n"
                    + "            soi.value 'sc_shipping_fee',\n"
                    + "            so.coupon_code,\n"
                    + "            sovt.name 'coupon_type',\n"
                    + "            soi.cart_rule_display_names,\n"
                    + "            sois.name 'last_status',\n"
                    + "            so.created_at 'order_date',\n"
                    + "            MIN(IF(soish.fk_sales_order_item_status = 5, soish.created_at, NULL)) 'first_shipped_date',\n"
                    + "            MAX(IF(soish.fk_sales_order_item_status = 5, soish.created_at, NULL)) 'last_shipped_date',\n"
                    + "            MIN(IF(soish.fk_sales_order_item_status = 27, soish.created_at, NULL)) 'delivered_date',\n"
                    + "            MIN(IF(soish.fk_sales_order_item_status = 44, soish.created_at, NULL)) 'not_delivered_date',\n"
                    + "            MIN(IF(soish.fk_sales_order_item_status = 6, soish.created_at, NULL)) 'closed_date',\n"
                    + "            MIN(IF(soish.fk_sales_order_item_status = 56, soish.created_at, NULL)) 'refund_completed_date',\n"
                    + "            ppt.name 'pickup_provider_type',\n"
                    + "            soi.id_package_dispatching,\n"
                    + "            NULL AS 'invoice_tracking_number',"
                    + "            NULL AS 'invoice_shipment_provider',"
                    + "            soi.first_tracking_number,\n"
                    + "            soi.first_shipment_provider,\n"
                    + "            soi.last_tracking_number,\n"
                    + "            soi.last_shipment_provider,\n"
                    + "            sfom.origin 'origin',\n"
                    + "            soa.city,\n"
                    + "            dst.id_customer_address_region 'id_district',\n"
                    + "            IF(cspu.length * cspu.width * cspu.height IS NOT NULL\n"
                    + "                OR cspu.weight IS NOT NULL, IFNULL(cspu.length, 0), IFNULL(cc.package_length, 0)) 'length',\n"
                    + "            IF(cspu.length * cspu.width * cspu.height IS NOT NULL\n"
                    + "                OR cspu.weight IS NOT NULL, IFNULL(cspu.width, 0), IFNULL(cc.package_width, 0)) 'width',\n"
                    + "            IF(cspu.length * cspu.width * cspu.height IS NOT NULL\n"
                    + "                OR cspu.weight IS NOT NULL, IFNULL(cspu.height, 0), IFNULL(cc.package_height, 0)) 'height',\n"
                    + "            IF(cspu.length * cspu.width * cspu.height IS NOT NULL\n"
                    + "                OR cspu.weight IS NOT NULL, IFNULL(cspu.weight, 0), IFNULL(cc.package_weight, 0)) 'weight'\n"
                    + "    FROM\n"
                    + "        (SELECT \n"
                    + "        result.*,\n"
                    + "            soi.*,\n"
                    + "            scsoi.id_sales_order_item 'sc_sales_order_item',\n"
                    + "            SUM(tr.value) 'value'\n"
                    + "    FROM\n"
                    + "        (SELECT \n"
                    + "        pck.id_package,\n"
                    + "            pd.id_package_dispatching,\n"
                    + "            pdh.tracking_number 'first_tracking_number',\n"
                    + "            sp1.shipment_provider_name 'first_shipment_provider',\n"
                    + "            pd.tracking_number 'last_tracking_number',\n"
                    + "            sp2.shipment_provider_name 'last_shipment_provider'\n"
                    + "    FROM\n"
                    + "        (SELECT \n"
                    + "        fk_package\n"
                    + "    FROM\n"
                    + "        oms_live.oms_package_status_history\n"
                    + "    WHERE\n"
                    + "        created_at >= '" + start + "'\n"
                    + "            AND created_at < '" + end + "'\n"
                    + "            AND fk_package_status IN (4)\n"
                    + "    GROUP BY fk_package) result\n"
                    + "    LEFT JOIN oms_live.oms_package pck ON result.fk_package = pck.id_package\n"
                    + "    LEFT JOIN oms_live.oms_package_dispatching pd ON pck.id_package = pd.fk_package\n"
                    + "    LEFT JOIN oms_live.oms_package_dispatching_history pdh ON pd.id_package_dispatching = pdh.fk_package_dispatching\n"
                    + "        AND pdh.id_package_dispatching_history = (SELECT \n"
                    + "            MIN(id_package_dispatching_history)\n"
                    + "        FROM\n"
                    + "            oms_live.oms_package_dispatching_history hst\n"
                    + "        LEFT JOIN oms_live.oms_shipment_provider sp ON hst.fk_shipment_provider = sp.id_shipment_provider\n"
                    + "        WHERE\n"
                    + "            fk_package_dispatching = pd.id_package_dispatching\n"
                    + "                AND tracking_number IS NOT NULL)\n"
                    + "    LEFT JOIN oms_live.oms_shipment_provider sp1 ON pdh.fk_shipment_provider = sp1.id_shipment_provider\n"
                    + "    LEFT JOIN oms_live.oms_shipment_provider sp2 ON pd.fk_shipment_provider = sp2.id_shipment_provider\n"
                    + "    HAVING first_shipment_provider IS NOT NULL\n"
                    + "        AND last_shipment_provider IS NOT NULL) result\n"
                    + "    LEFT JOIN oms_live.oms_package_item pi ON result.id_package = pi.fk_package\n"
                    + "    LEFT JOIN oms_live.ims_sales_order_item soi ON pi.fk_sales_order_item = soi.id_sales_order_item\n"
                    + "    LEFT JOIN screport.sales_order_item scsoi ON soi.id_sales_order_item = scsoi.src_id\n"
                    + "    LEFT JOIN screport.transaction tr ON scsoi.id_sales_order_item = tr.ref\n"
                    + "        AND tr.fk_transaction_type = 7\n"
                    + "    GROUP BY soi.id_sales_order_item) soi\n"
                    + "    LEFT JOIN oms_live.ims_sales_order so ON soi.fk_sales_order = so.id_sales_order\n"
                    + "    LEFT JOIN oms_live.ims_sales_order_item_status sois ON soi.fk_sales_order_item_status = sois.id_sales_order_item_status\n"
                    + "    LEFT JOIN oms_live.ims_sales_order_item_status_history soish ON soi.id_sales_order_item = soish.fk_sales_order_item\n"
                    + "        AND soish.fk_sales_order_item_status IN (5 , 27, 44, 6, 56)\n"
                    + "    LEFT JOIN oms_live.ims_supplier osup ON soi.bob_id_supplier = osup.bob_id_supplier\n"
                    + "    LEFT JOIN oms_live.oms_pickup_provider_type ppt ON osup.fk_pickup_provider_type = ppt.id_pickup_provider_type\n"
                    + "    LEFT JOIN oms_live.ims_sales_order_voucher_type sovt ON so.fk_voucher_type = sovt.id_sales_order_voucher_type\n"
                    + "    LEFT JOIN oms_live.ims_sales_order_address soa ON so.fk_sales_order_address_shipping = soa.id_sales_order_address\n"
                    + "    LEFT JOIN oms_live.ims_customer_address_region dst ON soa.fk_customer_address_region = dst.id_customer_address_region\n"
                    + "    LEFT JOIN bob_live.catalog_simple cs ON soi.sku = cs.sku\n"
                    + "    LEFT JOIN bob_live.catalog_config cc ON cc.id_catalog_config = cs.fk_catalog_config\n"
                    + "    LEFT JOIN bob_live.catalog_simple_package_unit cspu ON cspu.fk_catalog_simple = cs.id_catalog_simple\n"
                    + "    LEFT JOIN bob_live.supplier sup ON soi.bob_id_supplier = sup.id_supplier\n"
                    + "    LEFT JOIN bob_live.supplier_address sa ON sup.id_supplier = sa.fk_supplier\n"
                    + "        AND sa.id_supplier_address = (SELECT \n"
                    + "            MAX(id_supplier_address)\n"
                    + "        FROM\n"
                    + "            bob_live.supplier_address\n"
                    + "        WHERE\n"
                    + "            fk_supplier = sup.id_supplier\n"
                    + "                AND fk_country_region IS NOT NULL)\n"
                    + "    LEFT JOIN bob_live.shipping_fee_origin_mapping sfom ON sa.fk_country_region = sfom.fk_country_region\n"
                    + "        AND sfom.id_shipping_fee_origin_mapping = (SELECT \n"
                    + "            MAX(id_shipping_fee_origin_mapping)\n"
                    + "        FROM\n"
                    + "            bob_live.shipping_fee_origin_mapping\n"
                    + "        WHERE\n"
                    + "            fk_country_region = sa.fk_country_region)\n"
                    + "    LEFT JOIN screport.seller sel ON sup.id_supplier = sel.src_id\n"
                    + "    GROUP BY soi.id_sales_order_item\n"
                    + "    HAVING first_shipped_date >= '" + start + "'\n"
                    + "        AND first_shipped_date < '" + end + "') result) result) result";

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

            CSVWriter writer = new CSVWriter(new FileWriter(outFile + "\\" + "SCGL_"
                    + start + "_" + end + "_" + filenum + ".csv"));

            String[] resultrow = new String[col];

            while (rs.next()) {
                if (row == 1000000) {
                    filenum++;
                    row = 0;
                    writer.close();
                    writer = new CSVWriter(new FileWriter(outFile + "\\" + "SCGL_"
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
