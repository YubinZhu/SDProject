package app.service;

import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by yubzhu on 19-7-11
 */

public class DatabaseService {

    private static final String driver = "org.postgresql.Driver";

    private static final String ip = "100.64.137.141";

    private static final String port = "5432";

    private static final String database = "sdproject";

    private static final String user = "postgres";

    private static final String password = "";

    private static final int MAX_THREADS = 32;

    private static final ExecutorService executorService = Executors.newFixedThreadPool(MAX_THREADS);

    public Future<Integer> executeUpdate(String sqlSentence) {
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

    public Future<ResultSet> executeQuery(String sqlSentence) {
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
}
