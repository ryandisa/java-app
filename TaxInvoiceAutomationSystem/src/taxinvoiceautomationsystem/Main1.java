/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package taxinvoiceautomationsystem;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 *
 * @author Refly IDFA
 */
public class Main1 {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        ConnectionManagerLocalhost cm = new ConnectionManagerLocalhost();
        Connection conn = cm.open();
        Statement stmt;
        ResultSet rs;

        try {
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);

            int i = stmt.executeUpdate("TRUNCATE TABLE ship_calc_env.anondb_extract;");
            i = stmt.executeUpdate("LOAD DATA LOCAL INFILE\n"
                    + "	'C:\\\\Shipment_Charges_Gain_Loss\\\\ANONDB\\\\01_scgl_20161001_delv.csv'\n"
                    + "IGNORE INTO TABLE\n"
                    + "	ship_calc_env.anondb_extract\n"
                    + "COLUMNS TERMINATED BY\n"
                    + "	','\n"
                    + "OPTIONALLY ENCLOSED BY\n"
                    + "	'\"'\n"
                    + "IGNORE 1 ROWS;");
            System.out.println(i);
            cm.close();
        } catch (Exception e) {
            System.out.println("DB Error: " + e.getMessage());
        }
    }
}
