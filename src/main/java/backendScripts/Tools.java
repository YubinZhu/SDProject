package backendScripts;

import java.sql.*;
import java.util.concurrent.*;

/**
 * Created by yubzhu on 2019/7/5
 */

public class Tools extends Thread {

    private static final String driver = "org.postgresql.Driver";

    private static final String ip = "100.64.137.141";

    private static final String port = "5432";

    private static final String database = "sdproject";

    private static final String user = "postgres";

    private static final String password = "";

    static int executeUpdate(String sqlSentence) throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        Connection connection = DriverManager.getConnection("jdbc:postgresql://" + ip + ":" + port + "/" + database, user, password);
        Statement statement = connection.createStatement();
        int result = statement.executeUpdate(sqlSentence);
        connection.close(); // very important
        return result;
    }

    private String threadSqlSentence;

    private String threadSerialString;

    Tools(String sqlSentence, String serialString) {
        threadSqlSentence = sqlSentence;
        threadSerialString = serialString;
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
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        try {
            int result = executorService.submit(new UpdateExecutor(threadSqlSentence)).get(1, TimeUnit.MINUTES);
            System.out.println("#" + threadSerialString + ": " + result + " on {" + threadSqlSentence + "}");
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            System.out.println("#" + threadSerialString + ": execute update failed.");
        }
        executorService.shutdown();
    }
}
