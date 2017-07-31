/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package scheduledexportcsv;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 * @author Refly IDFA
 */
public class ConnectionManager {

    private Connection conn = null;

    public Connection open(String username, String password) throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
        System.out.println("Connecting to database...");
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        conn = DriverManager.getConnection("jdbc:mysql://idanonymous2.iddc:3306?zeroDateTimeBehavior=convertToNull&user=" + username + "&password=" + password);
        return conn;
    }
}
