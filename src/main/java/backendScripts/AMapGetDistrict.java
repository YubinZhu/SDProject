package backendScripts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URL;

import static app.configure.ApplicationConfigure.*;

/**
 * Created by yubzhu on 19-8-14
 */

class AMapGetDistrict {

    static ObjectNode getDistrict(String keyword, int subDistrict, String extensions) {
        try {
            URL url = new URL(districtUrl + "?keywords=" + keyword + "&subdistrict=" + subDistrict + "&extensions=" + extensions + "&key=" + key);
            return (ObjectNode)new ObjectMapper().readTree(url);
        } catch (IOException | NullPointerException e) {
            return null;
        }
    }
}
