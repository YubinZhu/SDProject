package app.controller;

import app.service.LogService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

import java.io.IOException;
import java.net.URL;

/**
 * Created by yubzhu on 2019/7/9
 */

@RestController
@CrossOrigin(origins = "*")
@RequestMapping("/location")
public class LocationController {

    @Autowired
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private LogService log = new LogService(LocationController.class);

    private static final String address = "https://restapi.amap.com/v3/geocode/geo";

    private static final String key = "c346ef3fe374bf57803d4eb57aca0fb0";

    private static final String backupKey = "f4edf4d440e4de85a51cb04a37586532";

    private static final boolean batch = false;

    @GetMapping("/geo")
    public ObjectNode queryLocation(HttpServletRequest httpServletRequest,
                                    @RequestParam(value = "address") String string) {
        ObjectNode objectNode = objectMapper.createObjectNode();
        try {
            if (string == null || string.equals("")) {
                throw new NullPointerException();
            }
            URL url = new URL(address + "?address=" + string.replace("#", "号").replace(" ", "") + "&key=" + key + "&batch=" + batch);
            String location = objectMapper.readValue(url, ObjectNode.class).get("geocodes").get(0).get("location").asText();
            objectNode.put("lon", location.split(",")[0]);
            objectNode.put("lat", location.split(",")[1]);
            log.printQueryOkInfo(httpServletRequest);
        } catch (NullPointerException | IOException e) {
            log.printExceptionOccurredWarning(httpServletRequest, e);
            objectNode.removeAll();
            objectNode.put("exception", e.getClass().getSimpleName());
        }
        return objectNode;
    }

}
