/*
 * Dette java program fremprovokerer "dirty read" på en database.
 * "Dirty read" er når en transaktion læser en værdi på fra en database som ikke er committed, og derfor kan ændres
 * som følge af rollback.
 *
 * Dette program opsætter to tråde (t1 og t2) med hver sin transaktion.
 * t1 læser fra et felt, mens t2 laver pådate og rollback på samme felt.
 * Afhængig af transaktionsisolationsniveauet kan der ske dirty reads.
 * Hvis t1 selecter værdien 66, er der sket et dirty read.
 *
 * Programmet kan bruges til at eksperimentere med transaktioners påvirkning af hinanden, ved
 * brug af commit, rollback og transaktionsisolationsniveauer.
 *
 * Der anvendes en database med en "Account" tabel med felterne "id" og "balance"
 *
 * EK feb. 2023
 */

package com.example.TransactionDirtyRead;

import java.sql.*;

/**
 *
 * @author ek
 */
public class TransactionDirtyRead {

    public static void main(String[] args) throws Exception {

        isolationlevelTest();

    }

    private static void isolationlevelTest() throws SQLException {

        // For at arbejde med to transaktioner benyttes to connections til databasen.

        try {
            Connection conn1 = DBConnector.getConnection();

            // Først sættes balance til 1000 så vi er sikre på udgangspunktet.
            try {
                String sql = "UPDATE Account SET Balance = 1000 where id=1";
                Statement stmt = conn1.createStatement();
                stmt.executeUpdate(sql);
                System.out.println("UPDATE: Account 1 til balance = 1000");
                System.out.println("------------------------------------");
                conn1.commit();
            } catch (Exception e) {
                System.out.println(e);
            }

            // Her sættes transaktionsisolationsniveauet
            // Dirty read kan kun ske på laveste niveau, dvs TRANSACTION_READ_UNCOMMITTED
            System.out.println("T1 Default Trans.Iso.level = " + conn1.getTransactionIsolation());
            //conn1.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            //conn1.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED); // Default
            //conn1.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            conn1.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            System.out.println("T1 Trans.Iso.level sættes = " + conn1.getTransactionIsolation());

            // TRANSAKTION 1
            // Laver select på Account tabellen 10 gange
            System.out.println("T1 start transaktion");
            //conn1.setAutoCommit(false);   // BEMÆRK det er på conn1
            Thread t1 = new Thread(() -> {
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

            // TRANSAKTION 2
            // Laver update på account tabellen uden commit, men med rollback.
            Connection conn2 = DBConnector.getConnection();
            System.out.println("T2 start transaktion");
            Thread t2 = new Thread(() -> {
                System.out.println("T2 start");
                try {
                    conn2.setAutoCommit(false);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                    try {
                        String sql = "UPDATE Account SET Balance = 66 where id=1";
                        Statement stmt = conn2.createStatement();
                        stmt.executeUpdate(sql);
                        System.out.println("T2 UPDATE id=1, balance = 66");
                        //System.out.println("T2 committer");
                        //conn2.commit();
                        System.out.println("T2 laver rollback");
                        conn2.rollback();
                    } catch (Exception e) {
                        System.out.println(e);
                    }
                try {
                    conn2.setAutoCommit(false);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                System.out.println("T2 slut");
            });

            t2.start();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}