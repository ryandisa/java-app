/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package scheduledewireport;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

/**
 *
 * @author Refly IDFA
 */
public class EWIReportDAO extends TimerTask {

    private List<EWIReport> ewiReport;
    private Date extractStart, extractEnd;
    private String outFile;
    private Object caller;
    private PropertyChangeSupport changes;
    private int status;
    public static final String PROPERTY_STATUS = "EWI_REPORT_STATUS";
    public static final String PROPERTY_PROGRESS = "EWI_REPORT_PROGRESS";
    public static final String PROPERTY_RESULT = "EWI_REPORT_RESULT";
    public static final int STAT_INIT = 0;
    public static final int STAT_CONNECT_DB = 1;
    public static final int STAT_EXECUTE_QUERY = 2;
    public static final int STAT_FETCH_RESULT = 3;
    public static final int STAT_DISCONNECT_DB = 4;
    public static final int STAT_CONVERT_XLSX = 5;
    public static final int STAT_WRITE_XLSX = 6;
    public static final int STAT_DONE = 7;

    public EWIReportDAO(Date extractStart, Date extractEnd, String outFile) {
        this.extractStart = extractStart;
        this.extractEnd = extractEnd;
        this.outFile = outFile;
        this.changes = new PropertyChangeSupport(this);
        this.ewiReport = new ArrayList<>();
    }

    public List<EWIReport> getEwiReport() {
        return ewiReport;
    }

    public void setEwiReport(List<EWIReport> ewiReport) {
        this.ewiReport = ewiReport;
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

    public Object getCaller() {
        return caller;
    }

    public void setCaller(Object caller) {
        this.caller = caller;
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

    public void extractEWIReport() {
        changes.firePropertyChange(PROPERTY_STATUS, status, STAT_CONNECT_DB);
        status = STAT_CONNECT_DB;

        ConnectionManager cm = new ConnectionManager();
        Connection conn = cm.open();

        System.out.println("Executing query...");
        changes.firePropertyChange(PROPERTY_STATUS, status, STAT_EXECUTE_QUERY);
        status = STAT_EXECUTE_QUERY;

        Statement stmt;
        ResultSet rs;
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String start = dateFormat.format(this.extractStart);
        String end = dateFormat.format(this.extractEnd);

        try {
            String sql = "SELECT \n"
                    + "    *\n"
                    + "FROM\n"
                    + "    (SELECT \n"
                    + "        so.order_nr 'order_number',\n"
                    + "            soi.bob_id_sales_order_item 'sales_order_item',\n"
                    + "            soi.id_sales_order_item 'sap_item_id',\n"
                    + "            sc.id_sales_order_item 'sc_sales_order_item',\n"
                    + "            so.payment_method 'payment_method',\n"
                    + "            sois.name 'item_status',\n"
                    + "            so.created_at 'order_date',\n"
                    + "            MIN(IF(soish.fk_sales_order_item_status = 67, soish.created_at, NULL)) 'verified_date',\n"
                    + "            MIN(IF(soish.fk_sales_order_item_status = 5, soish.created_at, NULL)) 'shipped_date',\n"
                    + "            MIN(IF(soish.fk_sales_order_item_status = 27, soish.real_action_date, NULL)) 'delivered_date',\n"
                    + "            MIN(IF(soish.fk_sales_order_item_status = 27, soish.created_at, NULL)) 'delivered_date_input',\n"
                    + "            MIN(IF(soish.fk_sales_order_item_status = 68, soish.created_at, NULL)) 'returned_date',\n"
                    + "            MIN(IF(soish.fk_sales_order_item_status = 78, soish.created_at, NULL)) 'replaced_date',\n"
                    + "            MIN(IF(soish.fk_sales_order_item_status = 56, soish.created_at, NULL)) 'refunded_date',\n"
                    + "            soi.unit_price,\n"
                    + "            soi.paid_price,\n"
                    + "            soi.coupon_money_value,\n"
                    + "            soi.cart_rule_discount,\n"
                    + "            soi.shipping_amount,\n"
                    + "            soi.shipping_surcharge,\n"
                    + "            sc.item_price_credit,\n"
                    + "            sc.commission,\n"
                    + "            sc.payment_fee,\n"
                    + "            sc.shipping_fee_credit,\n"
                    + "            sc.item_price,\n"
                    + "            sc.commission_credit,\n"
                    + "            sc.shipping_fee,\n"
                    + "            sc.seller_credit,\n"
                    + "            sc.seller_credit_item,\n"
                    + "            sc.other_fee,\n"
                    + "            sc.seller_debit_item,\n"
                    + "            (sc.item_price_credit + sc.commission + sc.payment_fee + sc.shipping_fee_credit + sc.item_price + sc.commission_credit + sc.shipping_fee + sc.seller_credit + sc.seller_credit_item + sc.other_fee + sc.seller_debit_item) 'amount_paid_seller',\n"
                    + "            sc.transaction_id 'transaction_id',\n"
                    + "            sc.transaction_date,\n"
                    + "            sc.start_date 'statement_time_frame_start',\n"
                    + "            sc.end_date 'statement_time_frame_end',\n"
                    + "            soi.sku,\n"
                    + "            soa.fk_customer_address_region 'district_id',\n"
                    + "            pd.tracking_number 'awb',\n"
                    + "            sp.shipment_provider_name 'shipment_provider',\n"
                    + "            sc.sc_id_seller,\n"
                    + "            sc.seller_name,\n"
                    + "            sc.legal_name,\n"
                    + "            sc.tax_class\n"
                    + "    FROM\n"
                    + "        (SELECT \n"
                    + "        ts.id_transaction_statement,\n"
                    + "            ts.start_date,\n"
                    + "            ts.end_date,\n"
                    + "            ts.created_at 'statement_date',\n"
                    + "            MIN(tr.created_at) 'transaction_date',\n"
                    + "            SUM(IF(tr.fk_transaction_type = 13, tr.value, 0)) 'item_price_credit',\n"
                    + "            SUM(IF(tr.fk_transaction_type = 16, tr.value, 0)) 'commission',\n"
                    + "            SUM(IF(tr.fk_transaction_type = 3, tr.value, 0)) 'payment_fee',\n"
                    + "            SUM(IF(tr.fk_transaction_type = 8, tr.value, 0)) 'shipping_fee_credit',\n"
                    + "            SUM(IF(tr.fk_transaction_type = 14, tr.value, 0)) 'item_price',\n"
                    + "            SUM(IF(tr.fk_transaction_type = 15, tr.value, 0)) 'commission_credit',\n"
                    + "            SUM(IF(tr.fk_transaction_type = 7, tr.value, 0)) 'shipping_fee',\n"
                    + "            SUM(IF(tr.fk_transaction_type = 19, tr.value, 0)) 'seller_credit',\n"
                    + "            SUM(IF(tr.fk_transaction_type = 36, tr.value, 0)) 'seller_credit_item',\n"
                    + "            SUM(IF(tr.fk_transaction_type = 20, tr.value, 0)) 'other_fee',\n"
                    + "            SUM(IF(tr.fk_transaction_type = 37, tr.value, 0)) 'seller_debit_item',\n"
                    + "            soi.id_sales_order_item,\n"
                    + "            soi.src_id,\n"
                    + "            tr.number 'transaction_id',\n"
                    + "            ts.number 'statement_id',\n"
                    + "            sel.tax_class,\n"
                    + "            sel.short_code 'sc_id_seller',\n"
                    + "            sel.name 'seller_name',\n"
                    + "            sel.name_company 'legal_name'\n"
                    + "    FROM\n"
                    + "        screport.transaction tr\n"
                    + "    LEFT JOIN screport.transaction_statement ts ON ts.id_transaction_statement = tr.fk_transaction_statement\n"
                    + "    LEFT JOIN screport.sales_order_item soi ON tr.ref = soi.id_sales_order_item\n"
                    + "    LEFT JOIN screport.seller sel ON soi.fk_seller = sel.id_seller\n"
                    + "    WHERE\n"
                    + "        tr.created_at >= STR_TO_DATE('" + start + "', '%Y-%m-%d')\n"
                    + "            AND tr.created_at < STR_TO_DATE('" + end + "', '%Y-%m-%d')\n"
                    + "            AND tr.fk_transaction_type IN (3 , 7, 8, 13, 14, 15, 16, 20, 26)\n"
                    + "    GROUP BY tr.ref) sc\n"
                    + "    LEFT JOIN oms_live.ims_sales_order_item soi ON sc.src_id = soi.id_sales_order_item\n"
                    + "    LEFT JOIN oms_live.ims_sales_order so ON soi.fk_sales_order = so.id_sales_order\n"
                    + "    LEFT JOIN oms_live.ims_sales_order_address soa ON so.fk_sales_order_address_shipping = soa.id_sales_order_address\n"
                    + "    LEFT JOIN oms_live.ims_sales_order_item_status sois ON soi.fk_sales_order_item_status = sois.id_sales_order_item_status\n"
                    + "    LEFT JOIN oms_live.ims_sales_order_item_status_history soish ON soi.id_sales_order_item = soish.fk_sales_order_item\n"
                    + "        AND soish.fk_sales_order_item_status IN (5 , 27, 56, 67, 68, 78)\n"
                    + "    LEFT JOIN oms_live.oms_package_item pi ON soi.id_sales_order_item = pi.fk_sales_order_item\n"
                    + "    LEFT JOIN oms_live.oms_package_dispatching pd ON pi.fk_package = pd.fk_package\n"
                    + "    LEFT JOIN oms_live.oms_shipment_provider sp ON pd.fk_shipment_provider = sp.id_shipment_provider\n"
                    + "    WHERE\n"
                    + "        soi.id_sales_order_item IS NOT NULL\n"
                    + "    GROUP BY soi.id_sales_order_item) result";

            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);
            rs = stmt.executeQuery(sql);

            EWIReport temp;
            changes.firePropertyChange(PROPERTY_STATUS, status, STAT_FETCH_RESULT);
            status = STAT_FETCH_RESULT;
            int i = 0;

            while (rs.next()) {
                temp = new EWIReport();
                temp.setOrderNumber(rs.getInt("order_number"));
                temp.setSalesOrderItem(rs.getInt("sales_order_item"));
                temp.setSapItemID(rs.getInt("sap_item_id"));
                temp.setScSalesOrderItem(rs.getInt("sc_sales_order_item"));
                temp.setPaymentMethod(rs.getString("payment_method"));
                temp.setItemStatus(rs.getString("item_status"));
                temp.setOrderDate(rs.getDate("order_date"));
                temp.setVerifiedDate(rs.getDate("verified_date"));
                temp.setShippedDate(rs.getDate("shipped_date"));
                temp.setDeliveredDate(rs.getDate("delivered_date"));
                temp.setDeliveredDateInput(rs.getDate("delivered_date_input"));
                temp.setReturnedDate(rs.getDate("returned_date"));
                temp.setReplacedDate(rs.getDate("replaced_date"));
                temp.setRefundedDate(rs.getDate("refunded_date"));
                temp.setUnitPrice(rs.getDouble("unit_price"));
                temp.setPaidPrice(rs.getDouble("paid_price"));
                temp.setCouponMoneyValue(rs.getDouble("coupon_money_value"));
                temp.setCartRuleDiscount(rs.getDouble("cart_rule_discount"));
                temp.setShippingAmount(rs.getDouble("shipping_amount"));
                temp.setShippingSurcharge(rs.getDouble("shipping_surcharge"));
                temp.setItemPriceCredit(rs.getDouble("item_price_credit"));
                temp.setCommission(rs.getDouble("commission"));
                temp.setPaymentFee(rs.getDouble("payment_fee"));
                temp.setShippingFeeCredit(rs.getDouble("shipping_fee_credit"));
                temp.setItemPrice(rs.getDouble("item_price"));
                temp.setCommissionCredit(rs.getDouble("commission_credit"));
                temp.setShippingFee(rs.getDouble("shipping_fee"));
                temp.setSellerCredit(rs.getDouble("seller_credit"));
                temp.setSellerCreditItem(rs.getDouble("seller_credit_item"));
                temp.setOtherFee(rs.getDouble("other_fee"));
                temp.setSellerDebitItem(rs.getDouble("seller_debit_item"));
                temp.setAmountPaidSeller(rs.getDouble("amount_paid_seller"));
                temp.setTransactionID(rs.getString("transaction_id"));
                temp.setTransactionDate(rs.getDate("transaction_date"));
                temp.setStatementTimeFrameStart(rs.getDate("statement_time_frame_start"));
                temp.setStatementTimeFrameEnd(rs.getDate("statement_time_frame_end"));
                temp.setSku(rs.getString("sku"));
                temp.setDistrictID(rs.getInt("district_id"));
                temp.setAwb(rs.getString("awb"));
                temp.setShipmentProvider(rs.getString("shipment_provider"));
                temp.setScIDSeller(rs.getString("sc_id_seller"));
                temp.setSellerName(rs.getString("seller_name"));
                temp.setLegalName(rs.getString("legal_name"));
                temp.setTaxClass(rs.getString("tax_class"));

                ewiReport.add(temp);
                changes.firePropertyChange(PROPERTY_PROGRESS, i, i + 1);
                i++;
            }

            changes.firePropertyChange(PROPERTY_STATUS, status, STAT_DISCONNECT_DB);
            status = STAT_DISCONNECT_DB;
            System.out.println("Closing database connection...");
            rs.close();
            stmt.close();
            conn.close();
        } catch (Exception ex) {
            Logger.getLogger(EWIReportDAO.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
        }
    }

    public void write() {
        changes.firePropertyChange(PROPERTY_STATUS, status, STAT_CONVERT_XLSX);
        status = STAT_CONVERT_XLSX;

        //Blank workbook
        SXSSFWorkbook workbook = new SXSSFWorkbook();

        //Create a blank sheet
        SXSSFSheet sheet = workbook.createSheet("EWI Report");
        sheet.setRandomAccessWindowSize(1000);

        int rownum = 0;
        Row row = sheet.createRow(rownum++);

        System.out.println("Generating Data...");

        row.createCell(0).setCellValue("order_number");
        row.createCell(1).setCellValue("sales_order_item");
        row.createCell(2).setCellValue("sap_item_id");
        row.createCell(3).setCellValue("sc_sales_order_item");
        row.createCell(4).setCellValue("payment_method");
        row.createCell(5).setCellValue("item_status");
        row.createCell(6).setCellValue("order_date");
        row.createCell(7).setCellValue("verified_date");
        row.createCell(8).setCellValue("shipped_date");
        row.createCell(9).setCellValue("delivered_date");
        row.createCell(10).setCellValue("delivered_date_input");
        row.createCell(11).setCellValue("returned_date");
        row.createCell(12).setCellValue("replaced_date");
        row.createCell(13).setCellValue("refunded_date");
        row.createCell(14).setCellValue("unit_price");
        row.createCell(15).setCellValue("paid_price");
        row.createCell(16).setCellValue("coupon_money_value");
        row.createCell(17).setCellValue("cart_rule_discount");
        row.createCell(18).setCellValue("shipping_amount");
        row.createCell(19).setCellValue("shipping_surcharge");
        row.createCell(20).setCellValue("item_price_credit");
        row.createCell(21).setCellValue("commission");
        row.createCell(22).setCellValue("payment_fee");
        row.createCell(23).setCellValue("shipping_fee_credit");
        row.createCell(24).setCellValue("item_price");
        row.createCell(25).setCellValue("commission_credit");
        row.createCell(26).setCellValue("shipping_fee");
        row.createCell(27).setCellValue("seller_credit");
        row.createCell(28).setCellValue("seller_credit_item");
        row.createCell(29).setCellValue("other_fee");
        row.createCell(30).setCellValue("seller_debit_item");
        row.createCell(31).setCellValue("amount_paid_seller");
        row.createCell(32).setCellValue("transaction_id");
        row.createCell(33).setCellValue("transaction_date");
        row.createCell(34).setCellValue("statement_time_frame_start");
        row.createCell(35).setCellValue("statement_time_frame_end");
        row.createCell(36).setCellValue("sku");
        row.createCell(37).setCellValue("district_id");
        row.createCell(38).setCellValue("awb");
        row.createCell(39).setCellValue("shipment_provider");
        row.createCell(40).setCellValue("sc_id_seller");
        row.createCell(41).setCellValue("seller_name");
        row.createCell(42).setCellValue("legal_name");
        row.createCell(43).setCellValue("tax_class");

        int i = 0;

        for (EWIReport ewireport : ewiReport) {
            row = sheet.createRow(rownum++);

            row.createCell(0).setCellValue(ewireport.getOrderNumber());
            row.createCell(1).setCellValue(ewireport.getSalesOrderItem());
            row.createCell(2).setCellValue(ewireport.getSapItemID());
            row.createCell(3).setCellValue(ewireport.getScSalesOrderItem());
            row.createCell(4).setCellValue(ewireport.getPaymentMethod());
            row.createCell(5).setCellValue(ewireport.getItemStatus());
            row.createCell(6).setCellValue(String.valueOf(ewireport.getOrderDate()));
            row.createCell(7).setCellValue(String.valueOf(ewireport.getVerifiedDate()));
            row.createCell(8).setCellValue(String.valueOf(ewireport.getShippedDate()));
            row.createCell(9).setCellValue(String.valueOf(ewireport.getDeliveredDate()));
            row.createCell(10).setCellValue(String.valueOf(ewireport.getDeliveredDateInput()));
            row.createCell(11).setCellValue(String.valueOf(ewireport.getReturnedDate()));
            row.createCell(12).setCellValue(String.valueOf(ewireport.getReplacedDate()));
            row.createCell(13).setCellValue(String.valueOf(ewireport.getRefundedDate()));
            row.createCell(14).setCellValue(ewireport.getUnitPrice());
            row.createCell(15).setCellValue(ewireport.getPaidPrice());
            row.createCell(16).setCellValue(ewireport.getCouponMoneyValue());
            row.createCell(17).setCellValue(ewireport.getCartRuleDiscount());
            row.createCell(18).setCellValue(ewireport.getShippingAmount());
            row.createCell(19).setCellValue(ewireport.getShippingSurcharge());
            row.createCell(20).setCellValue(ewireport.getItemPriceCredit());
            row.createCell(21).setCellValue(ewireport.getCommission());
            row.createCell(22).setCellValue(ewireport.getPaymentFee());
            row.createCell(23).setCellValue(ewireport.getShippingFeeCredit());
            row.createCell(24).setCellValue(ewireport.getItemPrice());
            row.createCell(25).setCellValue(ewireport.getCommissionCredit());
            row.createCell(26).setCellValue(ewireport.getShippingFee());
            row.createCell(27).setCellValue(ewireport.getSellerCredit());
            row.createCell(28).setCellValue(ewireport.getSellerCreditItem());
            row.createCell(29).setCellValue(ewireport.getOtherFee());
            row.createCell(30).setCellValue(ewireport.getSellerDebitItem());
            row.createCell(31).setCellValue(ewireport.getAmountPaidSeller());
            row.createCell(32).setCellValue(ewireport.getTransactionID());
            row.createCell(33).setCellValue(String.valueOf(ewireport.getTransactionDate()));
            row.createCell(34).setCellValue(String.valueOf(ewireport.getStatementTimeFrameStart()));
            row.createCell(35).setCellValue(String.valueOf(ewireport.getStatementTimeFrameEnd()));
            row.createCell(36).setCellValue(ewireport.getSku());
            row.createCell(37).setCellValue(ewireport.getDistrictID());
            row.createCell(38).setCellValue(ewireport.getAwb());
            row.createCell(39).setCellValue(ewireport.getShipmentProvider());
            row.createCell(40).setCellValue(ewireport.getScIDSeller());
            row.createCell(41).setCellValue(ewireport.getSellerName());
            row.createCell(42).setCellValue(ewireport.getLegalName());
            row.createCell(43).setCellValue(ewireport.getTaxClass());

            changes.firePropertyChange(PROPERTY_PROGRESS, i, i + 1);
            i++;
        }

        try {
            changes.firePropertyChange(PROPERTY_STATUS, status, STAT_WRITE_XLSX);
            status = STAT_WRITE_XLSX;

            DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
            String start = dateFormat.format(extractStart);
            String end = dateFormat.format(extractEnd);

            System.out.println("Writing data into file...");
            
            System.out.println(outFile);
            
            outFile = outFile + "\\";
            
            String file = outFile + "EWI_REPORT_" + start + "_" + end + ".csv";
            FileOutputStream out = new FileOutputStream(new File(file));
            workbook.write(out);
            out.close();

            changes.firePropertyChange(PROPERTY_STATUS, status, STAT_DONE);
            status = STAT_DONE;
            System.out.println("Done!");
        } catch (Exception e) {
        }

        init();
    }

    public void init() {
        ewiReport = new ArrayList<>();
        extractStart = new Date();
        extractEnd = new Date();
        changes.firePropertyChange(PROPERTY_STATUS, status, STAT_INIT);
        status = STAT_INIT;
    }

    @Override
    public void run() {
        if (extractStart != null && extractEnd != null) {
            extractEWIReport();
            write();
        }
    }
}
