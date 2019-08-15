package app.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URL;

import static app.configure.ApplicationConfigure.*;

/**
 * Created by yubzhu on 2019/7/12
 */

public class AMapService {

    public static String getGeo(String address) {
        try {
            URL url = new URL(geoUrl + "?address=" + address.replace("#", "号").replace(" ", "") + "&key=" + key + "&batch=" + batch);
            return new ObjectMapper().readTree(url).get("geocodes").get(0).get("location").asText();
        } catch (IOException | NullPointerException e) {
            return null;
        }
    }

    static ObjectNode getDistrict(String keyword, int subDistrict, String extensions) {
        try {
            URL url = new URL(districtUrl + "?keywords=" + keyword + "&subdistrict=" + subDistrict + "&extensions=" + extensions + "&key=" + key);
            return (ObjectNode)new ObjectMapper().readTree(url);
        } catch (IOException | NullPointerException e) {
            return null;
        }
    }
}
