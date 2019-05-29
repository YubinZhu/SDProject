package app.service;

import java.sql.*;

/**
 * Created by yubzhu on 19-5-15
 */

public class QueryTableService {

    private static final String driver = "org.postgresql.Driver";

    private static final String ip = "100.64.137.141";

    private static final String port = "5432";

    private static final String database = "industry";

    private static final String user = "postgres";

    private static final String password = "";

    public static ResultSet query(String sqlSentence) throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        Connection connection = DriverManager.getConnection("jdbc:postgresql://" + ip + ":" + port + "/" + database, user, password);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sqlSentence);
        connection.close(); // very important
        return resultSet;
    }
}
