package backendScripts;

import java.sql.*;
import java.util.concurrent.*;

import static app.configure.ApplicationConfigure.*;

/**
 * Created by yubzhu on 2019/7/5
 */

public class DatabaseExecutor extends Thread {

    private String threadSqlSentence;

    private String threadSerialString;

    DatabaseExecutor(String sqlSentence, String serialString) {
        threadSqlSentence = sqlSentence;
        threadSerialString = serialString;
    }

    static ResultSet executeQuery(String sqlSentence) throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        Connection connection = DriverManager.getConnection("jdbc:postgresql://" + ip + ":" + port + "/" + database, user, password);
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(sqlSentence);
        connection.close();
        return result;
    }

    static int executeUpdate(String sqlSentence) throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        Connection connection = DriverManager.getConnection("jdbc:postgresql://" + ip + ":" + port + "/" + database, user, password);
        Statement statement = connection.createStatement();
        int result = statement.executeUpdate(sqlSentence);
        connection.close(); // very important
        return result;
    }

    class UpdateExecutor implements Callable<Integer> {

        private String sqlSentence;

        UpdateExecutor(String sqlSentence) {
            this.sqlSentence = sqlSentence;
        }

        @Override
        public Integer call() {
            try {
                return executeUpdate(sqlSentence);
            } catch (ClassNotFoundException | SQLException e) {
                return null;
            }
        }
    }

    @Override
    public void run() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            int result = executorService.submit(new UpdateExecutor(threadSqlSentence)).get(timeoutInterval, timeoutTimeUnit);
            System.out.println("#" + threadSerialString + ": " + result + " on {" + threadSqlSentence + "}");
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            System.out.println("#" + threadSerialString + ": execute update failed.");
        }
        executorService.shutdown();
    }
}
