import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;


 class DeadlockSimulation {

    private static final String POSTGRESQL_URL = "jdbc:postgresql://localhost:32768/unblu";
    private static final String MSSQL_URL = "jdbc:sqlserver://localhost:1433;databaseName=unblu;encrypt=false";
    private static final String ORACLE_URL = "jdbc:oracle:thin:@//127.0.0.1:1521/oracledb";

     private static final String USER = "unblu";
    private static final String POSTGRES_PASSWORD = "unblume";
     private static final String MSSQL_PASSWORD = "S3cur3#Acc3ss!";

     private static final String ORACLE_USER = "system";

     private static final String ORACLE_PASSWORD = "unblume";

     private static final int MAX_RETRIES = 5;
    private static boolean enableRetry = true;

    String ID1="";
    String ID2="";

     public DeadlockSimulation() {

    }

    public void startSimulation() {
        ID1 = getTrackingItemIdByType("AGENT");
        ID2 = getTrackingItemIdByType("TRACKINGLIST");
         System.out.println(" id1: "+ID1+",id2:"+ID2);
        Thread thread1 = new Thread(() -> handleTransaction(1));
        Thread thread2 = new Thread(() -> handleTransaction(2));
        thread1.start();
        thread2.start();
    }

     public  String getTrackingItemIdByType(String trackingItemType)  {
         String query = "SELECT id FROM unblu.live_trackingitem WHERE tracking_item_type = ?";
         String id = "";
         try (Connection connection = getConnection();
              PreparedStatement preparedStatement = connection.prepareStatement(query)) {
             preparedStatement.setString(1, trackingItemType);
             try (ResultSet resultSet = preparedStatement.executeQuery()) {
                 if (resultSet.next()) {
                     id = resultSet.getString("id");
                 }
             }
         } catch (SQLException e) {
             e.printStackTrace();
         }

         return id;
     }

    private void handleTransaction(int threadId) {
        int attempts = 0;
        boolean success = false;

        while (attempts < MAX_RETRIES) {  // && !success) {
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
                e.printStackTrace();
                try {
                    Thread.sleep(5000);
                }catch(Exception e1) {}
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
      // return DriverManager.getConnection(POSTGRESQL_URL, USER, POSTGRES_PASSWORD);
       return DriverManager.getConnection(MSSQL_URL, USER, MSSQL_PASSWORD);
       // return DriverManager.getConnection(ORACLE_URL, ORACLE_USER, ORACLE_PASSWORD);
    }

    private void executeThread1Operations(Connection conn) throws SQLException, InterruptedException {
        try {
            try (PreparedStatement pstmt1 = conn.prepareStatement("UPDATE unblu.live_trackingitem SET tracking_item_status = tracking_item_status  WHERE id = \'"+ID1+"\'")) { //='CHAT'
                pstmt1.executeUpdate();
                Thread.sleep(5000);
                try (PreparedStatement pstmt2 = conn.prepareStatement("UPDATE unblu.live_trackingitem SET tracking_item_status = tracking_item_status  WHERE id = \'"+ID2+"\'")) { //'ACCEPTED'
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
            try (PreparedStatement pstmt1 = conn.prepareStatement("UPDATE unblu.live_trackingitem SET tracking_item_status = tracking_item_status WHERE id = \'"+ID2+"\'")) {
                pstmt1.executeUpdate();
                Thread.sleep(4000);
                try (PreparedStatement pstmt2 = conn.prepareStatement("UPDATE unblu.live_trackingitem SET tracking_item_status = tracking_item_status  WHERE id = \'"+ID1+"\'")) {
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

    public static void main(String[] args) throws InterruptedException {
       test = new DeadlockSimulation();
       test.startSimulation();
       System.out.println("****");
    }
}