package backendScripts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URL;
import java.sql.*;

/**
 * Created by yubzhu on 2019/7/5
 */

public class Tools {

    private static ObjectMapper objectMapper = new ObjectMapper();

    private static final String address = "https://restapi.amap.com/v3/geocode/geo";

    private static final String key = "c346ef3fe374bf57803d4eb57aca0fb0";

    private static final String backupKey = "f4edf4d440e4de85a51cb04a37586532";

    private static final boolean batch = false;

    public static String getLocation(String string) {
        try {
            URL url = new URL(address + "?address=" + string.replace("#", "号") +"&key=" + key + "&batch=" + batch);
            ObjectNode objectNode = objectMapper.readValue(url, ObjectNode.class);
            try {
                return objectNode.get("geocodes").get(0).get("location").toString();
            } catch (NullPointerException e) {
                if (objectNode.get("status").toString().equals("0")) {
                    System.out.println("status 0 at query {" + string + "}");
                } else if (objectNode.get("count").toString().equals("0")) {
                    System.out.println("count 0 at query {" + string + "}");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
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
        System.out.println(result + " on {" + sqlSentence + "}");
        connection.close(); // very important
        return result;
    }
}
