package app.controller;

import app.service.LogService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static app.service.DatabaseService.getResultSet;

/**
 * Created by yubzhu on 2019/9/5
 */

@RestController
@CrossOrigin(origins = "*")
@RequestMapping("/fivehundred")
public class FiveHundredController {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final LogService log = new LogService(FiveHundredController.class);

    @GetMapping("/category")
    public ObjectNode queryCategory(HttpServletRequest httpServletRequest) {
        return null;
    }

    @GetMapping("/coordinates")
    public ObjectNode queryCoordinates(HttpServletRequest httpServletRequest) {
        try {
            ObjectNode objectNode = objectMapper.createObjectNode();
            String sqlSentence = "select id, lon, lat from five_hundred order by id asc";
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
            String sqlSentence = "select id, name from five_hundred where name ~* '" + name + "' order by id asc";
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
            String sqlSentence = "select * from five_hundred where id = " + id;
            ResultSet resultSet = getResultSet(sqlSentence);
            if (resultSet.next()) {
                objectNode.put("id", resultSet.getInt("id"));
                objectNode.put("rank_2018", resultSet.getInt("rank_2018"));
                objectNode.put("rank_2017", resultSet.getInt("rank_2017"));
                objectNode.put("name", resultSet.getString("name"));
                objectNode.put("income", resultSet.getDouble("income"));
                objectNode.put("profit", resultSet.getDouble("profit"));
                objectNode.put("address", resultSet.getString("address"));
                objectNode.put("chairman", resultSet.getString("chairman"));
                objectNode.put("sec_code", resultSet.getString("sec_code"));
                objectNode.put("employee_num", resultSet.getInt("employee_num"));
                objectNode.put("code", resultSet.getString("code"));
                objectNode.put("website", resultSet.getString("website"));
                objectNode.put("pure_assets", resultSet.getDouble("pure_assets"));
                objectNode.put("assets", resultSet.getDouble("assets"));
                objectNode.put("value", resultSet.getDouble("value"));
                objectNode.put("pure_profit_rate", resultSet.getDouble("pure_profit_rate"));
                objectNode.put("pure_assets_rate", resultSet.getDouble("pure_assets_rate"));
                objectNode.put("income_rate", resultSet.getDouble("income_rate"));
                objectNode.put("profit_rate", resultSet.getDouble("profit_rate"));
                objectNode.put("lon", resultSet.getDouble("lon"));
                objectNode.put("lat", resultSet.getDouble("lat"));
            }
            log.printExecuteOkInfo(httpServletRequest);
            return objectNode;
        } catch (InterruptedException | ExecutionException | TimeoutException | SQLException | NullPointerException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }

    @GetMapping("/heatmap")
    public ObjectNode queryHeatmap(HttpServletRequest httpServletRequest) {
        try {
            ObjectNode objectNode = objectMapper.createObjectNode();
            String sqlSentence = "select lon, lat from listed_company order by id asc";
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
            log.printExecuteOkInfo(httpServletRequest);
            return objectNode;
        } catch (InterruptedException | ExecutionException | TimeoutException | SQLException | NullPointerException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }

    @GetMapping("/statistics")
    public ObjectNode queryStatistics(HttpServletRequest httpServletRequest) {
        try {
            ObjectNode objectNode = objectMapper.createObjectNode();
            String sqlSentence = "select name, profit from five_hundred order by profit desc limit 20";
            ResultSet resultSet = getResultSet(sqlSentence);
            ObjectNode tempObjectNode = objectMapper.createObjectNode();
            while (resultSet.next()) {
                tempObjectNode.put(resultSet.getString("name"), resultSet.getDouble("profit"));
            }
            objectNode.set("profit_rank", tempObjectNode);
            sqlSentence = "select name, income from five_hundred order by income desc limit 20";
            resultSet = getResultSet(sqlSentence);
            tempObjectNode = objectMapper.createObjectNode();
            while (resultSet.next()) {
                tempObjectNode.put(resultSet.getString("name"), resultSet.getInt("income"));
            }
            objectNode.set("income_rank", tempObjectNode);
            log.printExecuteOkInfo(httpServletRequest);
            return objectNode;
        } catch (InterruptedException | ExecutionException | TimeoutException | SQLException | NullPointerException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }
}
