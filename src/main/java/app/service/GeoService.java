package app.service;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URL;

import static app.configure.ApplicationConfigure.address;
import static app.configure.ApplicationConfigure.key;
import static app.configure.ApplicationConfigure.batch;

/**
 * Created by yubzhu on 2019/7/12
 */

public class GeoService {

    public static String getLocation(String string) {
        try {
            URL url = new URL(address + "?address=" + string.replace("#", "号").replace(" ", "") + "&key=" + key + "&batch=" + batch);
            return new ObjectMapper().readTree(url).get("geocodes").get(0).get("location").asText();
        } catch (IOException | NullPointerException e) {
            return null;
        }
    }
}
