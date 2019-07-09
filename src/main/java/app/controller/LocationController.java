package app.controller;

import app.service.LogService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

import static backendScripts.Tools.getLocation;

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

    @GetMapping("/geo")
    public ObjectNode queryLocation(HttpServletRequest httpServletRequest,
                                    @RequestParam(value = "address") String address) {
        ObjectNode objectNode = objectMapper.createObjectNode();
        String location = getLocation(address);
        if (location == null) {
            return null;
        }
        objectNode.put("lon", location.split(",")[0].split("\"")[1]);
        objectNode.put("lat", location.split(",")[1].split("\"")[0]);
        log.printQueryOkInfo(httpServletRequest);
        return objectNode;
    }

}
