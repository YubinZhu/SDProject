package app.service;

import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.concurrent.*;

import static app.configure.ApplicationConfigure.*;

/**
 * Created by yubzhu on 19-7-11
 */

public class DatabaseService {

    private static final ExecutorService executorService = Executors.newFixedThreadPool(maxThreads);

    private Future<Integer> executeUpdate(String sqlSentence) {
        return executorService.submit(new UpdateExecutor(sqlSentence));
    }

    class UpdateExecutor implements Callable<Integer> {
        private String sqlSentence;

        UpdateExecutor(String sqlSentence) {
            this.sqlSentence = sqlSentence;
        }

        @Override
        public Integer call() {
            try {
                Class.forName(driver);
                Connection connection = DriverManager.getConnection("jdbc:postgresql://" + ip + ":" + port + "/" + database, user, password);
                Statement statement = connection.createStatement();
                Integer result = statement.executeUpdate(sqlSentence);
                connection.close();
                return result;
            } catch (ClassNotFoundException | SQLException e) {
                LoggerFactory.getLogger(this.getClass()).error(e.getClass().getName());
                return null;
            }
        }
    }

    private Future<ResultSet> executeQuery(String sqlSentence) {
        return executorService.submit(new QueryExecutor(sqlSentence));
    }

    class QueryExecutor implements Callable<ResultSet> {
        private String sqlSentence;

        QueryExecutor(String sqlSentence) {
            this.sqlSentence = sqlSentence;
        }

        @Override
        public ResultSet call() {
            try {
                Class.forName(driver);
                Connection connection = DriverManager.getConnection("jdbc:postgresql://" + ip + ":" + port + "/" + database, user, password);
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery(sqlSentence);
                connection.close();
                return result;
            } catch (ClassNotFoundException | SQLException e) {
                LoggerFactory.getLogger(this.getClass()).error(e.getClass().getName());
                return null;
            }
        }
    }

    public static ResultSet getResultSet(String sqlSentence) throws InterruptedException, ExecutionException, TimeoutException {
        return new DatabaseService().executeQuery(sqlSentence).get(timeoutInterval, timeoutTimeUnit);
    }
}
