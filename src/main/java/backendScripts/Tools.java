package backendScripts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URL;
import java.sql.*;
import java.util.concurrent.*;

/**
 * Created by yubzhu on 2019/7/5
 */

public class Tools extends Thread {

    private static ObjectMapper objectMapper = new ObjectMapper();

    private static final String address = "https://restapi.amap.com/v3/geocode/geo";

    private static final String key = "c346ef3fe374bf57803d4eb57aca0fb0";

    private static final String backupKey = "f4edf4d440e4de85a51cb04a37586532";

    private static final boolean batch = false;

    static String getLocation(String string, String serialString) {
        if (string == null || string.equals("")) {
            System.out.println("#" + serialString + ": empty address.");
        } else {
            try {
                URL url = new URL(address + "?address=" + string.replace("#", "号").replace(" ", "") + "&key=" + key + "&batch=" + batch);
                ObjectNode objectNode = objectMapper.readValue(url, ObjectNode.class);
                try {
                    return objectNode.get("geocodes").get(0).get("location").asText();
                } catch (NullPointerException e) {
                    System.out.println("#" + serialString + ": get location failed.");
                }
            } catch (IOException e) {
                System.out.println("#" + serialString + ": unknown error.");
            }
        }
        return null;
    }

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
        try {
            int result = Executors.newFixedThreadPool(1).submit(new UpdateExecutor(threadSqlSentence)).get(1, TimeUnit.MINUTES);
            System.out.println("#" + threadSerialString + ": " + result + " on {" + threadSqlSentence + "}");
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            System.out.println("#" + threadSerialString + ": execute update failed.");
        }
    }
}
