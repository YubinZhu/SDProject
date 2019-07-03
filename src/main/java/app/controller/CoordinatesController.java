package app.controller;

import app.service.LogService;
import app.service.QueryTableService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by yubzhu on 2019/6/9
 */

@RestController
@CrossOrigin(origins = "*")
@RequestMapping("/coordinates")
public class CoordinatesController {

    @Autowired
    private static ObjectMapper objectMapper = new ObjectMapper();

    private static LogService log = new LogService(CoordinatesController.class);

    @GetMapping("/qingdao")
    public ObjectNode queryQingDaoCompanyCoordinates(HttpServletRequest httpServletRequest,
                                              @RequestParam(required = false, value = "label") String label,
                                              @RequestParam(required = false, value = "industry") String industry) {
        ObjectNode objectNode = objectMapper.createObjectNode();
        try {
            String sqlSentence = "select ent_id, ent_industry, lon, lat from ent_info";
            if (label != null && industry != null) {
                sqlSentence += " where ent_label = '" + label + "' and ent_industry = '" + industry + "'";
            } else if (label != null) {
                sqlSentence += " where ent_label = '" + label + "'";
            } else if (industry != null) {
                sqlSentence += " where ent_industry = '" + industry + "'";
            }
            sqlSentence += " order by ent_id asc";
            ResultSet resultSet = QueryTableService.query(sqlSentence);
            ArrayNode arrayNode = objectMapper.createArrayNode();
            while (resultSet.next()) {
                ObjectNode featureObjectNode = objectMapper.createObjectNode();
                featureObjectNode.put("type", "Feature");
                featureObjectNode.put("id", resultSet.getInt("ent_id"));
                featureObjectNode.put("industry", resultSet.getString("ent_industry"));
                ObjectNode geometryObjectNode = objectMapper.createObjectNode();
                geometryObjectNode.put("type", "Point");
                ArrayNode coordArrayNode = objectMapper.createArrayNode();
                coordArrayNode.add(resultSet.getDouble("lon"));
                coordArrayNode.add(resultSet.getDouble("lat"));
                geometryObjectNode.set("coordinates", coordArrayNode);
                featureObjectNode.set("geometry", geometryObjectNode);
                arrayNode.add(featureObjectNode);
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
    public ObjectNode queryListedCompanyCoordinates(HttpServletRequest httpServletRequest,
                                                    @RequestParam(required = false, value = "type") String type,
                                                    @RequestParam(required = false, value = "province") String province,
                                                    @RequestParam(required = false, value = "city") String city) {
        ObjectNode objectNode = objectMapper.createObjectNode();
        try {
            String sqlSentence = "select id, lon, lat, industry_type from comp_info";
            if (type != null || province != null || city != null) {
                sqlSentence += " where id is not null"; // in order to use 'and'
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
            sqlSentence += " order by id asc";
            ResultSet resultSet = QueryTableService.query(sqlSentence);
            ArrayNode arrayNode = objectMapper.createArrayNode();
            while (resultSet.next()) {
                ObjectNode featureObjectNode = objectMapper.createObjectNode();
                featureObjectNode.put("type", "Feature");
                featureObjectNode.put("id", resultSet.getInt("id"));
                featureObjectNode.put("company_type", resultSet.getString("industry_type"));
                ObjectNode geometryObjectNode = objectMapper.createObjectNode();
                geometryObjectNode.put("type", "Point");
                ArrayNode coordArrayNode = objectMapper.createArrayNode();
                coordArrayNode.add(resultSet.getDouble("lon"));
                coordArrayNode.add(resultSet.getDouble("lat"));
                geometryObjectNode.set("coordinates", coordArrayNode);
                featureObjectNode.set("geometry", geometryObjectNode);
                arrayNode.add(featureObjectNode);
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
