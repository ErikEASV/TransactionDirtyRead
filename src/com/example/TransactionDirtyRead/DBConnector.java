package com.example.TransactionDirtyRead;

import java.sql.Connection;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerException;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 *
 * @author smsj
 */
public class DBConnector {

    public final static String DB_DRIVER_CLASS = "BankAccount";
    public final static String DB_URL = "localhost";
    public final static String DB_USERNAME = "sa";
    public final static String DB_PASSWORD = "U6u7U8u5yY";

    /**
     *
     * @return
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static Connection getConnection() throws SQLException, ClassNotFoundException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setDatabaseName("BankAccount");
        ds.setUser("sa");
        ds.setPassword("U6u7U8u5yY");
        ds.setPortNumber(1433);
        ds.setServerName("localhost");
        ds.setTrustServerCertificate(true);

        Connection conn = ds.getConnection();

        return conn;
    }
}

