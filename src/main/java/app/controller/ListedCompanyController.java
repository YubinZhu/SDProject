package app.controller;

import app.service.DatabaseService;
import app.service.GeoService;
import app.service.LogService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by yubzhu on 19-7-11
 */

@RestController
@CrossOrigin(origins = "*")
@RequestMapping("/listed")
public class ListedCompanyController {

    @Autowired
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final LogService log = new LogService(ListedCompanyController.class);

    private static final long timeout = 1;

    private static ResultSet getResultSet(String sqlSentence) throws InterruptedException, ExecutionException, TimeoutException {
        return new DatabaseService().executeQuery(sqlSentence).get(timeout, TimeUnit.MINUTES);
    }

    @GetMapping("/category")
    public ObjectNode queryCategoty(HttpServletRequest httpServletRequest) {
        try {
            ObjectNode objectNode = objectMapper.createObjectNode();
            String sqlSentence = "select distinct(industrial_type) from listed_company where industrial_type is not null order by industrial_type asc";
            ResultSet resultSet = getResultSet(sqlSentence);
            ArrayNode arrayNode = objectMapper.createArrayNode();
            while (resultSet.next()) {
                arrayNode.add(resultSet.getString("industrial_type"));
            }
            objectNode.set("type", arrayNode);
            sqlSentence = "select distinct(province) from listed_company where province is not null order by province asc";
            resultSet = getResultSet(sqlSentence);
            ObjectNode tempObjectNode = objectMapper.createObjectNode();
            while (resultSet.next()) {
                sqlSentence = "select distinct(city) from listed_company where city is not null and province = '" + resultSet.getString("province") + "' order by city asc";
                ResultSet tempResultSet = getResultSet(sqlSentence);
                arrayNode = objectMapper.createArrayNode();
                while (tempResultSet.next()) {
                    arrayNode.add(tempResultSet.getString("city"));
                }
                tempObjectNode.set(resultSet.getString("province"), arrayNode);
            }
            objectNode.set("province", tempObjectNode);
            log.printExecuteOkInfo(httpServletRequest);
            return objectNode;
        } catch (InterruptedException | ExecutionException | TimeoutException | SQLException | NullPointerException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }

    @GetMapping("/coordinates")
    public ObjectNode queryCoordinates(HttpServletRequest httpServletRequest,
                                       @RequestParam(required = false, value = "type") String type,
                                       @RequestParam(required = false, value = "province") String province,
                                       @RequestParam(required = false, value = "city") String city) {
        try {
            ObjectNode objectNode = objectMapper.createObjectNode();
            String sqlSentence = "select id, lon, lat, industrial_type from listed_company";
            if (type != null || province != null || city != null) {
                sqlSentence += " where id is not null"; // in order to use 'and'
                if (type != null) {
                    sqlSentence += " and industrial_type = '" + type + "'";
                }
                if (province != null) {
                    sqlSentence += " and province = '" + province + "'";
                }
                if (city != null) {
                    sqlSentence += " and city = '" + city + "'";
                }
            }
            sqlSentence += " order by id asc";
            ResultSet resultSet = getResultSet(sqlSentence);
            ArrayNode arrayNode = objectMapper.createArrayNode();
            while (resultSet.next()) {
                ObjectNode featureObjectNode = objectMapper.createObjectNode();
                featureObjectNode.put("type", "Feature");
                featureObjectNode.put("id", resultSet.getInt("id"));
                featureObjectNode.put("industrial_type", resultSet.getString("industrial_type"));
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
            log.printExecuteOkInfo(httpServletRequest);
            return objectNode;
        } catch (InterruptedException | ExecutionException | TimeoutException | SQLException | NullPointerException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }

    @GetMapping("/list")
    public ObjectNode queryList(HttpServletRequest httpServletRequest,
                                @RequestParam(value = "name") String name) {
        try {
            ObjectNode objectNode = objectMapper.createObjectNode();
            String sqlSentence = "select id, com_chn_name from listed_company where com_chn_name ~* '" + name + "' order by id asc limit 20";
            ResultSet resultSet = getResultSet(sqlSentence);
            ArrayNode arrayNode = objectMapper.createArrayNode();
            while (resultSet.next()) {
                ObjectNode tempObjectNode = objectMapper.createObjectNode();
                tempObjectNode.put("id", resultSet.getInt("id"));
                tempObjectNode.put("name", resultSet.getString("com_chn_name"));
                arrayNode.add(tempObjectNode);
            }
            objectNode.set("company", arrayNode);
            log.printExecuteOkInfo(httpServletRequest);
            return objectNode;
        } catch (InterruptedException | ExecutionException | TimeoutException | SQLException | NullPointerException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }

    @GetMapping("/information")
    public ObjectNode queryInformation(HttpServletRequest httpServletRequest,
                                       @RequestParam(value = "id") String id) {
        try {
            ObjectNode objectNode = objectMapper.createObjectNode();
            String sqlSentence = "select * from listed_company where id = " + id;
            ResultSet resultSet = getResultSet(sqlSentence);
            if (resultSet.next()) {
                objectNode.put("id", resultSet.getInt("id"));
                objectNode.put("name", resultSet.getString("com_chn_name"));
                objectNode.put("lon", resultSet.getDouble("lon"));
                objectNode.put("lat", resultSet.getDouble("lat"));
                objectNode.put("industrial_type", resultSet.getString("industrial_type"));
                objectNode.put("address", resultSet.getString("address"));
                objectNode.put("website", resultSet.getString("website"));
                objectNode.put("province", resultSet.getString("province"));
            }
            log.printExecuteOkInfo(httpServletRequest);
            return objectNode;
        } catch (InterruptedException | ExecutionException | TimeoutException | SQLException | NullPointerException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }

    @GetMapping("/heatmap")
    public ObjectNode queryHeatmap(HttpServletRequest httpServletRequest,
                                   @RequestParam(required = false, value = "type") String type,
                                   @RequestParam(required = false, value = "province") String province,
                                   @RequestParam(required = false, value = "city") String city) {
        try {
            ObjectNode objectNode = objectMapper.createObjectNode();
            String sqlSentence = "select lon, lat from listed_company"; // in order to use 'and'
            if (type != null || province != null || city != null) {
                sqlSentence += " where id is not null";
                if (type != null) {
                    sqlSentence += " and industrial_type = '" + type + "'";
                }
                if (province != null) {
                    sqlSentence += " and province = '" + province + "'";
                }
                if (city != null) {
                    sqlSentence += " and city = '" + city + "'";
                }
            }
            sqlSentence += " order by id asc";
            ResultSet resultSet = getResultSet(sqlSentence);
            objectNode.put("type", "FeatureCollection");
            ArrayNode arrayNode = objectMapper.createArrayNode();
            while (resultSet.next()) {
                ObjectNode tempObjectNode = objectMapper.createObjectNode();
                tempObjectNode.put("type", "Feature");
                tempObjectNode.put("weight", 1);
                ObjectNode insideObjectNode = objectMapper.createObjectNode();
                insideObjectNode.put("type", "Point");
                ArrayNode insideArrayNode = objectMapper.createArrayNode();
                insideArrayNode.add(resultSet.getDouble("lon"));
                insideArrayNode.add(resultSet.getDouble("lat"));
                insideObjectNode.set("coordinates", insideArrayNode);
                tempObjectNode.set("geometry", insideObjectNode);
                arrayNode.add(tempObjectNode);
            }
            objectNode.set("features", arrayNode);
            return objectNode;
        } catch (InterruptedException | ExecutionException | TimeoutException | SQLException | NullPointerException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }

    @GetMapping("/geo")
    public ObjectNode queryGeo(HttpServletRequest httpServletRequest,
                               @RequestParam(value = "address") String address) {
        try {
            ObjectNode objectNode = objectMapper.createObjectNode();
            String location = GeoService.getLocation(address);
            if (location == null) {
                throw new NullPointerException();
            }
            objectNode.put("lon", location.split(",")[0]);
            objectNode.put("lat", location.split(",")[1]);
            return objectNode;
        } catch (NullPointerException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }
}
