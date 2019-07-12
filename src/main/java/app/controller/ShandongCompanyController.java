package app.controller;

import app.service.DatabaseService;
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
 * Created by yubzhu on 2019/7/12
 */

@RestController
@CrossOrigin(origins = "*")
@RequestMapping("/shandong")
public class ShandongCompanyController {

    @Autowired
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final LogService log = new LogService(ShandongCompanyController.class);

    private static final long timeout = 1;

    private static ResultSet getResultSet(String sqlSentence) throws InterruptedException, ExecutionException, TimeoutException {
        return new DatabaseService().executeQuery(sqlSentence).get(timeout, TimeUnit.MINUTES);
    }

    @GetMapping("/category")
    public ObjectNode queryCategoty(HttpServletRequest httpServletRequest) {
        try {
            ObjectNode objectNode = objectMapper.createObjectNode();
            String sqlSentence = "select distinct(city) from shandong_company where city is not null order by city asc";
            ResultSet resultSet = getResultSet(sqlSentence);
            ObjectNode tempObjectNode = objectMapper.createObjectNode();
            while (resultSet.next()) {
                sqlSentence = "select distinct(county) from shandong_company where county is not null and city = '" + resultSet.getString("city") + "' order by county asc";
                ResultSet tempResultSet = getResultSet(sqlSentence);
                ArrayNode arrayNode = objectMapper.createArrayNode();
                while (tempResultSet.next()) {
                    arrayNode.add(tempResultSet.getString("county"));
                }
                tempObjectNode.set(resultSet.getString("city"), arrayNode);
            }
            objectNode.set("city", tempObjectNode);
            log.printExecuteOkInfo(httpServletRequest);
            return objectNode;
        } catch (InterruptedException | ExecutionException | TimeoutException | SQLException | NullPointerException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }

    @GetMapping("/coordinates")
    public ObjectNode queryCoordinates(HttpServletRequest httpServletRequest,
                                       @RequestParam(required = false, value = "city") String city,
                                       @RequestParam(required = false, value = "county") String county) {
        try {
            ObjectNode objectNode = objectMapper.createObjectNode();
            String sqlSentence = "select id, lon, lat from shandong_company";
            if (city != null || county != null) {
                sqlSentence += " where id is not null"; // in order to use 'and'
                if (city != null) {
                    sqlSentence += " and city = '" + city + "'";
                }
                if (county != null) {
                    sqlSentence += " and county = '" + county + "'";
                }
            }
            sqlSentence += " order by id asc";
            ResultSet resultSet = getResultSet(sqlSentence);
            ArrayNode arrayNode = objectMapper.createArrayNode();
            while (resultSet.next()) {
                ObjectNode featureObjectNode = objectMapper.createObjectNode();
                featureObjectNode.put("type", "Feature");
                featureObjectNode.put("id", resultSet.getInt("id"));
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
            String sqlSentence = "select id, name from shandong_company where name ~* '" + name + "' order by id asc limit 20";
            ResultSet resultSet = getResultSet(sqlSentence);
            ArrayNode arrayNode = objectMapper.createArrayNode();
            while (resultSet.next()) {
                ObjectNode tempObjectNode = objectMapper.createObjectNode();
                tempObjectNode.put("id", resultSet.getInt("id"));
                tempObjectNode.put("name", resultSet.getString("name"));
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
            String sqlSentence = "select * from shandong_company where id = " + id;
            ResultSet resultSet = getResultSet(sqlSentence);
            if (resultSet.next()) {
                objectNode.put("id", resultSet.getInt("id"));
                objectNode.put("name", resultSet.getString("name"));
                objectNode.put("lon", resultSet.getDouble("lon"));
                objectNode.put("lat", resultSet.getDouble("lat"));
                objectNode.put("lg_psn_name", resultSet.getString("lg_psn_name"));
                objectNode.put("address", resultSet.getString("address"));
                objectNode.put("website", resultSet.getString("website"));
                objectNode.put("city", resultSet.getString("city"));
                objectNode.put("county", resultSet.getString("county"));
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
                                   @RequestParam(required = false, value = "city") String city,
                                   @RequestParam(required = false, value = "county") String county) {
        try {
            ObjectNode objectNode = objectMapper.createObjectNode();
            String sqlSentence = "select lon, lat from shandong_company";
            if (city != null || county != null) {
                sqlSentence += " where id is not null"; // in order to use 'and'
                if (city != null) {
                    sqlSentence += " and city = '" + city + "'";
                }
                if (county != null) {
                    sqlSentence += " and county = '" + county + "'";
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
}
