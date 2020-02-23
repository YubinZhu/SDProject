package app.configure;

import java.util.concurrent.TimeUnit;

/**
 * Created by yubzhu on 19-7-11
 */

public class ApplicationConfigure {

    /* for controller */
    public static final int timeoutInterval = 30;

    public static final TimeUnit timeoutTimeUnit = TimeUnit.SECONDS;

    /* for database service */
    public static final String driver = "org.postgresql.Driver";

    public static final String ip = "100.64.137.141";
    //public static final String ip = "219.141.209.3";

    public static final String port = "5432";

    public static final String database = "sdproject";

    public static final String user = "postgres";

    public static final String password = "";

    public static final int maxThreads = 128;

    /* for geo service */
    public static final String geoUrl = "https://restapi.amap.com/v3/geocode/geo";

    public static final String districtUrl = "https://restapi.amap.com/v3/config/district";

    public static final String key = "c346ef3fe374bf57803d4eb57aca0fb0";

    public static final String backupKey = "f4edf4d440e4de85a51cb04a37586532";

    public static final boolean batch = false;
}
