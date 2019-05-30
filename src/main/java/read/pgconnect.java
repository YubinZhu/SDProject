package read;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.io.*;
import java.lang.String;

public class pgconnect {
    public static void main(String args[]) {
        Connection c = null;
        Statement stmt = null;
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://100.64.137.141:5432/industry",
                            "postgres","");
            System.out.println("Opened database successfully");
            String fileName ="misc\\青岛.csv";
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName),"GBK"));
            String tempString = null;
            stmt = c.createStatement();
            while ((tempString = reader.readLine()) != null) {
                String[] stringArray=tempString.split(",");
                String ent_name=stringArray[1];
                String ent_label=stringArray[2];
                String ent_industry=stringArray[3];
                System.out.print(stringArray.length);
                String sql;
                if(stringArray.length==6) {
                    String lat=stringArray[4];
                    String lon=stringArray[5];
                    GPS location = GPSConverterUtils.gcj_To_Gps84(Double.valueOf(lat), Double.valueOf(lon));
                    double new_lat = location.getLat();
                    double new_lon = location.getLon();
                    sql = "insert into ent_info (ent_label,ent_name,ent_industry,lon,lat) values ('" +
                            ent_label + "','" +
                            ent_name + "','" +
                            ent_industry + "','" +
                            new_lon + "','" +
                            new_lat + "');";
                }
                else{
                    sql= "insert into ent_info (ent_label,ent_name,ent_industry) values ('" +
                            ent_label + "','" +
                            ent_name + "','" +
                            ent_industry  + "');";
                }
//                System.out.println(sql);
                stmt.executeUpdate(sql);
            }
            reader.close();
            stmt.close();
            c.close();


        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }
    }
}
