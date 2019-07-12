package app.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URL;

/**
 * Created by yubzhu on 2019/7/12
 */

public class GeoService {

    private static final String address = "https://restapi.amap.com/v3/geocode/geo";

    private static final String key = "c346ef3fe374bf57803d4eb57aca0fb0";

    private static final String backupKey = "f4edf4d440e4de85a51cb04a37586532";

    private static final boolean batch = false;

    public static String getLocation(String string) {
        try {
            URL url = new URL(address + "?address=" + string.replace("#", "号").replace(" ", "") + "&key=" + key + "&batch=" + batch);
            return new ObjectMapper().readValue(url, ObjectNode.class).get("geocodes").get(0).get("location").asText();
        } catch (IOException | NullPointerException e) {
            return null;
        }
    }
}
