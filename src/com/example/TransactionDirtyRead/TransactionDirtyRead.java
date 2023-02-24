package com.example.TransactionDirtyRead;


import com.microsoft.sqlserver.jdbc.SQLServerException;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ek
 */
public class TransactionDirtyRead {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {

        transactionTest();
        //batchTest();
    }


    private static void transactionTest() throws SQLException {



        try {
            Connection conn1 = DBConnector.getConnection();

            conn1.setAutoCommit(false);
            try {
                String sql = "UPDATE Account SET Balance = 1000 where id=1";
                Statement stmt = conn1.createStatement();
                stmt.executeUpdate(sql);
                System.out.println("UPDATE: Account 1 til balance = 1000");
                conn1.commit();
            } catch (Exception e) {
                System.out.println(e);
            }

            System.out.println("T1 Default Trans.Iso.level = " + conn1.getTransactionIsolation());
            conn1.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            System.out.println("T1 Trans.Iso.level sættes = " + conn1.getTransactionIsolation());

            // TRANSAKTION 1
            System.out.println("T1 start transaktion");
            conn1.setAutoCommit(false);
            Thread t1 = new Thread(() -> {
                System.out.println("T1 start");
                for (int i=0; i<10; ++i)
                try {
                    String sql = "SELECT * FROM Account where id=1";
                    Statement stmt = conn1.createStatement();
                    ResultSet rs = stmt.executeQuery(sql);
                    while(rs.next()){
                        String id = rs.getString("id");
                        int balance = rs.getInt("balance");
                        System.out.println("T1 SELECT: id=" + id + ": "+ balance);
                    }

                } catch (Exception e) {
                    System.out.println(e);
                }

                System.out.println("T1 slut");
            });
            t1.start();
            conn1.commit();

            // TRANSAKTION 2
            Connection conn2 = DBConnector.getConnection();
            conn2.setAutoCommit(false);
            System.out.println("T2 start transaktion");
            Thread t2 = new Thread(() -> {
                System.out.println("T2 start");
                try {
                    String sql = "UPDATE Account SET Balance = 66 where id=1";
                    Statement stmt = conn2.createStatement();
                    stmt.executeUpdate(sql);
                    System.out.println("T2 UPDATE id=1, balance = 66");
                    System.out.println("T2 rollback");
                    //conn2.commit();
                    conn2.rollback();
                } catch (Exception e) {
                    System.out.println(e);
                }
                System.out.println("T2 slut");
            });
            t2.start();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }



    private static void batchTest() {
        try (Connection conn = new DBConnector().getConnection()) {
            PreparedStatement stmt = conn.prepareStatement("INSERT INTO Account VALUES (?,?)");

            for (int i = 0; i < 10000; i++) {
                stmt.setString(1, "Account #" + i);
                stmt.setInt(2, i);
                //stmt.addBatch();
                stmt.executeUpdate();
            }
            //stmt.executeBatch();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}