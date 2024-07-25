import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/*

-- postgres

CREATE TABLE mytable (
    id SERIAL PRIMARY KEY,
    value INT NOT NULL
);

INSERT INTO mytable (value) VALUES (0), (0);


-- sql server

CREATE TABLE dltable (
    id INT PRIMARY KEY,
    value INT NOT NULL
);

INSERT INTO dltable (id, value) VALUES (1, 0), (2, 0);

-- oracle

CREATE TABLE dltable (
    id NUMBER PRIMARY KEY,
    value NUMBER NOT NULL
);

INSERT INTO dltable (id, value) VALUES (1, 0);
INSERT INTO dltable (id, value) VALUES (2, 0);


 */
 class DeadlockSimulation {

    private static final String POSTGRESQL_URL = "jdbc:postgresql://localhost:32768/dldb";
    private static final String MSSQL_URL = "jdbc:sqlserver://localhost:1433;databaseName=dldb;encrypt=false";
    private static final String ORACLE_URL = "jdbc:oracle:thin:@//127.0.0.1:1521/oracledb";

     private static final String USER = "unblu";
    private static final String POSTGRES_PASSWORD = "unblume";
     private static final String MSSQL_PASSWORD = "S3cur3#Acc3ss!";

     private static final String ORACLE_USER = "system";

     private static final String ORACLE_PASSWORD = "unblume";

     private static final int MAX_RETRIES = 3;
    private static boolean enableRetry = true;

    public DeadlockSimulation() {
        Thread thread1 = new Thread(() -> handleTransaction(1));
        Thread thread2 = new Thread(() -> handleTransaction(2));
        thread1.start();
        thread2.start();
    }

    private void handleTransaction(int threadId) {
        int attempts = 0;
        boolean success = false;

        while (attempts < MAX_RETRIES && !success) {
            attempts++;
            try (Connection conn = getConnection()) {
                conn.setAutoCommit(false);
                if (threadId == 1) {
                    executeThread1Operations(conn);
                } else if (threadId == 2) {
                    executeThread2Operations(conn);
                }
                conn.commit();
                success = true;
                System.out.println("Thread " + threadId + " execution successful on attempt " + attempts);
            } catch (SQLException | InterruptedException e) {
                if (isDeadlock(e) && enableRetry) {
                    handleRetry(attempts, threadId);
                } else {
                    e.printStackTrace();
                    break;
                }
            }
        }
        if (!success) {
            System.err.println("Thread " + threadId + " failed to execute statement after " + MAX_RETRIES + " attempts due to deadlocks");
        }
    }
    private Connection getConnection() throws SQLException {
      //  return DriverManager.getConnection(POSTGRESQL_URL, USER, PASSWORD);
       // return DriverManager.getConnection(MSSQL_URL, USER, MSSQL_PASSWORD);
        return DriverManager.getConnection(ORACLE_URL, ORACLE_USER, ORACLE_PASSWORD);
    }

    private void executeThread1Operations(Connection conn) throws SQLException, InterruptedException {
        try {
            try (PreparedStatement pstmt1 = conn.prepareStatement("UPDATE dltable SET value = value + 1 WHERE id = 1")) {
                pstmt1.executeUpdate();
                Thread.sleep(2000);
                try (PreparedStatement pstmt2 = conn.prepareStatement("UPDATE dltable SET value = value + 1 WHERE id = 2")) {
                    pstmt2.executeUpdate();
                }
            }
        } catch (SQLException | InterruptedException e) {
            conn.rollback();
            throw e;
        }
    }

    private void executeThread2Operations(Connection conn) throws SQLException, InterruptedException {
        try {
            try (PreparedStatement pstmt1 = conn.prepareStatement("UPDATE dltable SET value = value + 1 WHERE id = 2")) {
                pstmt1.executeUpdate();
                Thread.sleep(2000);
                try (PreparedStatement pstmt2 = conn.prepareStatement("UPDATE dltable SET value = value + 1 WHERE id = 1")) {
                    pstmt2.executeUpdate();
                }
            }
        } catch (SQLException | InterruptedException e) {
            conn.rollback();
            throw e;
        }
    }

    private void handleRetry(int attempts, int threadId) {
        System.out.println("Deadlock detected in thread " + threadId + ", retrying... (attempt " + attempts + ")");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during retry wait", ie);
        }
    }

    private boolean isDeadlock(Exception e) {
        if (e instanceof SQLException) {
            String sqlState = ((SQLException) e).getSQLState();
            int errorCode = ((SQLException) e).getErrorCode();

            if (sqlState != null && sqlState.equals("40P01")) return true; //PostgreSQL

            switch (errorCode) {
                case  1213: return true; // MySql
                case  1205: return true; // SQL Server
                case  60: return true; // Oracle
            }


        }
        return false;
    }

}


public class Main {
    static DeadlockSimulation test;

    public static void main(String[] args) {
        test = new DeadlockSimulation();
    }
}