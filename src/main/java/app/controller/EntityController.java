package app.controller;

import app.domain.Dimension;
import app.domain.Filter;
import app.domain.Metric;
import app.domain.Request;
import app.service.LogService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static app.service.DatabaseService.getResultSet;

@RestController
@CrossOrigin(origins = "*")
@RequestMapping("/entity")
public class EntityController {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final LogService log = new LogService(EntityController.class);

    @GetMapping("/category")
    public ObjectNode queryCategory(HttpServletRequest httpServletRequest,
                                    @RequestParam(value = "label") String label) {
        try {
            ObjectNode result = objectMapper.createObjectNode();
            String sql = "select category from sd_entity_data where label = '" + label + "' and is_valid = true group by category order by category asc";
            ResultSet resultSet = getResultSet(sql);
            ArrayNode arrayNode = objectMapper.createArrayNode();
            while (resultSet.next()) {
                arrayNode.add(resultSet.getString("category"));
            }
            result.set("category", arrayNode);
            log.printExecuteOkInfo(httpServletRequest);
            return result;
        } catch (InterruptedException | ExecutionException | TimeoutException | SQLException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }

    @GetMapping("/statistic")
    public ObjectNode queryStatistic(HttpServletRequest httpServletRequest,
                                     @RequestParam(value = "request_json") String requestString) {
        try {
            ObjectNode result = objectMapper.createObjectNode();
            JsonNode requestJson = objectMapper.readTree(requestString);

            Request request = new Request(requestJson);
            request.fix();
            String sql = request.toSql();
            result.set("debug", objectMapper.createObjectNode().put("sql", sql));

            ResultSet response = getResultSet(sql);

            result.set("result", parseResult(response, request));

            log.printExecuteOkInfo(httpServletRequest);
            return result;
        } catch (IOException | InterruptedException | ExecutionException | TimeoutException | SQLException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }

    private JsonNode parseResult(ResultSet response, Request request) throws SQLException {
        if (response == null) {
            return null;
        }

        List<String> dimensionNames = new ArrayList<>();
        for (Dimension dimension : request.getDimensions()) {
            dimensionNames.add(dimension.getField());
        }
        List<String> metricNames = new ArrayList<>();
        for (Metric metric : request.getMetrics()) {
            String string = Optional.ofNullable(metric.getAlias()).orElse(metric.getField());
            metricNames.add(string == null ? "0" : string);
        }
        ObjectNode result = objectMapper.createObjectNode();

        while (response.next()) {
            ObjectNode pointer = result;
            for (String dimensionName : dimensionNames) {
                ArrayNode arrayNode = (ArrayNode)pointer.get(dimensionName);
                if (arrayNode == null) {
                    arrayNode = objectMapper.createArrayNode();
                    pointer.set(dimensionName, arrayNode);
                }
                boolean found = false;
                for (JsonNode jsonNode : arrayNode) {
                    if (jsonNode.get("value").asText().equals(response.getString(dimensionName))) {
                        pointer = (ObjectNode)jsonNode;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    ObjectNode objectNode = objectMapper.createObjectNode().put("value", response.getString(dimensionName));
                    arrayNode.add(objectNode);
                    pointer = objectNode;
                }
            }
            for (String metricName : metricNames) {
                pointer.put(metricName, response.getDouble(metricName));
            }
        }
        return result;
    }
}
