package oldApp.controller;

import oldApp.service.LogService;
import oldApp.service.QueryTableService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * Created by yubzhu on 2019/6/9
 */

@RestController
@CrossOrigin(origins = "*")
@RequestMapping("/heatmap")
public class HeatmapController {

    @Autowired
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final LogService log = new LogService(HeatmapController.class);

    @GetMapping("/qingdao")
    public ObjectNode queryQingDaoCompanyHeatmap(HttpServletRequest httpServletRequest) {
        ObjectNode objectNode = objectMapper.createObjectNode();
        try {
            String sqlSentence = "select lon, lat from ent_info";
            ResultSet resultSet = QueryTableService.query(sqlSentence);
            int heatmapPrecision = 4;
            HashMap<String, Integer> hashMap = new HashMap<>();
            while (resultSet.next()) {
                String lon = String.valueOf(resultSet.getDouble("lon"));
                String lat = String.valueOf(resultSet.getDouble("lat"));
                String index = lon.split("\\.")[0] + "." + lon.split("\\.")[1].concat("0000").substring(0, heatmapPrecision) + "," + lat.split("\\.")[0] + "." + lat.split("\\.")[1].concat("0000").substring(0, heatmapPrecision);
                hashMap.putIfAbsent(index, 0);
                hashMap.put(index, hashMap.get(index) + 1);
            }
            objectNode.put("type", "FeatureCollection");
            ArrayNode arrayNode = objectMapper.createArrayNode();
            for (String string : hashMap.keySet()) {
                ObjectNode tempObjectNode = objectMapper.createObjectNode();
                tempObjectNode.put("type", "Feature");
                tempObjectNode.put("weight", hashMap.get(string));
                ObjectNode insideObjectNode = objectMapper.createObjectNode();
                insideObjectNode.put("type", "Point");
                ArrayNode insideArrayNode = objectMapper.createArrayNode();
                insideArrayNode.add(Double.parseDouble(string.split(",")[0]));
                insideArrayNode.add(Double.parseDouble(string.split(",")[1]));
                insideObjectNode.set("coordinates", insideArrayNode);
                tempObjectNode.set("geometry", insideObjectNode);
                arrayNode.add(tempObjectNode);
            }
            objectNode.set("features", arrayNode);
            log.printQueryOkInfo(httpServletRequest);
        } catch (ClassNotFoundException | SQLException e) {
            log.printExceptionOccurredWarning(httpServletRequest, e);
            objectNode.removeAll();
            objectNode.put("exception", e.getClass().getSimpleName());
        }
        return objectNode;
    }

    @GetMapping("/listed")
    public ObjectNode queryListedCompanyHeatmap(HttpServletRequest httpServletRequest,
                                                @RequestParam(required = false, value = "type") String type,
                                                @RequestParam(required = false, value = "province") String province,
                                                @RequestParam(required = false, value = "city") String city) {
        ObjectNode objectNode = objectMapper.createObjectNode();
        try {
            String sqlSentence = "select lon, lat from comp_info"; // in order to use 'and'
            if (type != null || province != null || city != null) {
                sqlSentence += " where id is not null";
                if (type != null) {
                    sqlSentence += " and industry_type = '" + type + "'";
                }
                if (province != null) {
                    sqlSentence += " and province = '" + province + "'";
                }
                if (city != null) {
                    sqlSentence += " and city = '" + city + "'";
                }
            }
            ResultSet resultSet = QueryTableService.query(sqlSentence);
            int heatmapPrecision = 4;
            HashMap<String, Integer> hashMap = new HashMap<>();
            while (resultSet.next()) {
                String lon = String.valueOf(resultSet.getDouble("lon"));
                String lat = String.valueOf(resultSet.getDouble("lat"));
                String index = lon.split("\\.")[0] + "." + lon.split("\\.")[1].concat("0000").substring(0, heatmapPrecision) + "," + lat.split("\\.")[0] + "." + lat.split("\\.")[1].concat("0000").substring(0, heatmapPrecision);
                hashMap.putIfAbsent(index, 0);
                hashMap.put(index, hashMap.get(index) + 1);
            }
            objectNode.put("type", "FeatureCollection");
            ArrayNode arrayNode = objectMapper.createArrayNode();
            for (String string : hashMap.keySet()) {
                ObjectNode tempObjectNode = objectMapper.createObjectNode();
                tempObjectNode.put("type", "Feature");
                tempObjectNode.put("weight", hashMap.get(string));
                ObjectNode insideObjectNode = objectMapper.createObjectNode();
                insideObjectNode.put("type", "Point");
                ArrayNode insideArrayNode = objectMapper.createArrayNode();
                insideArrayNode.add(Double.parseDouble(string.split(",")[0]));
                insideArrayNode.add(Double.parseDouble(string.split(",")[1]));
                insideObjectNode.set("coordinates", insideArrayNode);
                tempObjectNode.set("geometry", insideObjectNode);
                arrayNode.add(tempObjectNode);
            }
            objectNode.set("features", arrayNode);
            log.printQueryOkInfo(httpServletRequest);
        } catch (ClassNotFoundException | SQLException e) {
            log.printExceptionOccurredWarning(httpServletRequest, e);
            objectNode.removeAll();
            objectNode.put("exception", e.getClass().getSimpleName());
        }
        return objectNode;
    }

}
