package app.controller;

import app.exception.IllegalParameterException;
import app.service.AMapService;
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
import java.util.concurrent.TimeoutException;

import static app.service.DatabaseService.getResultSet;

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
                ObjectNode tempObjectNode = objectMapper.createObjectNode();
                tempObjectNode.put("2018", resultSet.getDouble("income_2018"));
                tempObjectNode.put("2017", resultSet.getDouble("income_2017"));
                tempObjectNode.put("2016", resultSet.getDouble("income_2016"));
                tempObjectNode.put("2015", resultSet.getDouble("income_2015"));
                tempObjectNode.put("2014", resultSet.getDouble("income_2014"));
                objectNode.set("income", tempObjectNode);
                tempObjectNode = objectMapper.createObjectNode();
                tempObjectNode.put("2018", resultSet.getDouble("rsh_cost_2018"));
                tempObjectNode.put("2017", resultSet.getDouble("rsh_cost_2017"));
                tempObjectNode.put("2016", resultSet.getDouble("rsh_cost_2016"));
                tempObjectNode.put("2015", resultSet.getDouble("rsh_cost_2015"));
                tempObjectNode.put("2014", resultSet.getDouble("rsh_cost_2014"));
                objectNode.set("rsh_cost", tempObjectNode);
                tempObjectNode = objectMapper.createObjectNode();
                tempObjectNode.put("2018", resultSet.getDouble("gov_sub_2018"));
                tempObjectNode.put("2017", resultSet.getDouble("gov_sub_2017"));
                tempObjectNode.put("2016", resultSet.getDouble("gov_sub_2016"));
                tempObjectNode.put("2015", resultSet.getDouble("gov_sub_2015"));
                tempObjectNode.put("2014", resultSet.getDouble("gov_sub_2014"));
                objectNode.set("gov_sub", tempObjectNode);
                tempObjectNode = objectMapper.createObjectNode();
                tempObjectNode.put("2018", resultSet.getDouble("tax_2018"));
                tempObjectNode.put("2017", resultSet.getDouble("tax_2017"));
                tempObjectNode.put("2016", resultSet.getDouble("tax_2016"));
                tempObjectNode.put("2015", resultSet.getDouble("tax_2015"));
                tempObjectNode.put("2014", resultSet.getDouble("tax_2014"));
                objectNode.set("tax", tempObjectNode);
                tempObjectNode = objectMapper.createObjectNode();
                tempObjectNode.put("2018", resultSet.getDouble("profit_2018"));
                tempObjectNode.put("2017", resultSet.getDouble("profit_2017"));
                tempObjectNode.put("2016", resultSet.getDouble("profit_2016"));
                tempObjectNode.put("2015", resultSet.getDouble("profit_2015"));
                tempObjectNode.put("2014", resultSet.getDouble("profit_2014"));
                objectNode.set("profit", tempObjectNode);
                tempObjectNode = objectMapper.createObjectNode();
                tempObjectNode.put("2018", resultSet.getDouble("remission_2018"));
                tempObjectNode.put("2017", resultSet.getDouble("remission_2017"));
                tempObjectNode.put("2016", resultSet.getDouble("remission_2016"));
                tempObjectNode.put("2015", resultSet.getDouble("remission_2015"));
                tempObjectNode.put("2014", resultSet.getDouble("remission_2014"));
                objectNode.set("remission", tempObjectNode);
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
            String sqlSentence = "select lon, lat from listed_company";
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

    // todo: adjust order.
    @GetMapping("/geo")
    public ObjectNode queryGeo(HttpServletRequest httpServletRequest,
                               @RequestParam(value = "address") String address) {
        try {
            ObjectNode objectNode = objectMapper.createObjectNode();
            String location = AMapService.getGeo(address);
            if (location == null) {
                throw new NullPointerException();
            }
            objectNode.put("lon", location.split(",")[0]);
            objectNode.put("lat", location.split(",")[1]);
            String sqlSentence = "select id from listed_company where address ~* '" + address + "' order by id asc limit 20";
            ResultSet resultSet = getResultSet(sqlSentence);
            ArrayNode arrayNode = objectMapper.createArrayNode();
            while (resultSet.next()) {
                arrayNode.add(resultSet.getInt("id"));
            }
            objectNode.set("result", arrayNode);
            log.printExecuteOkInfo(httpServletRequest);
            return objectNode;
        } catch (InterruptedException | ExecutionException | TimeoutException | SQLException | NullPointerException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }

    @GetMapping("/statistic")
    public ObjectNode queryStatistic(HttpServletRequest httpServletRequest,
                                     @RequestParam(required = false, value = "type") String type,
                                     @RequestParam(required = false, value = "province") String province,
                                     @RequestParam(required = false, value = "city") String city,
                                     @RequestParam(required = false, value = "agg", defaultValue = "avg") String agg,
                                     @RequestParam(required = false, value = "histogram", defaultValue = "false") String histogram) {
        try {
            ObjectNode objectNode = objectMapper.createObjectNode();
            if (!(agg.equals("avg") || agg.equals("max") || agg.equals("min"))) {
                throw new IllegalParameterException();
            }
            if (!(histogram.equals("false") || histogram.equals("true"))) {
                throw new IllegalParameterException();
            }
            if (histogram.equals("true")) {
                if (type != null) {
                    throw new IllegalParameterException();
                }
                String sqlSentence = "select " + agg + "(income_2018) as income_2018, " + agg + "(income_2017) as income_2017, " +
                        agg + "(income_2016) as income_2016, " + agg + "(income_2015) as income_2015, " + agg + "(income_2014) as income_2014, " +
                        agg + "(rsh_cost_2018) as rsh_cost_2018, " + agg + "(rsh_cost_2017) as rsh_cost_2017, " +
                        agg + "(rsh_cost_2016) as rsh_cost_2016, " + agg + "(rsh_cost_2015) as rsh_cost_2015, " + agg + "(rsh_cost_2014) as rsh_cost_2014, " +
                        agg + "(gov_sub_2018) as gov_sub_2018, " + agg + "(gov_sub_2017) as gov_sub_2017, " +
                        agg + "(gov_sub_2016) as gov_sub_2016, " + agg + "(gov_sub_2015) as gov_sub_2015, " + agg + "(gov_sub_2014) as gov_sub_2014, " +
                        agg + "(tax_2018) as tax_2018, " + agg + "(tax_2017) as tax_2017, " +
                        agg + "(tax_2016) as tax_2016, " + agg + "(tax_2015) as tax_2015, " + agg + "(tax_2014) as tax_2014, " +
                        agg + "(profit_2018) as profit_2018, " + agg + "(profit_2017) as profit_2017, " +
                        agg + "(profit_2016) as profit_2016, " + agg + "(profit_2015) as profit_2015, " + agg + "(profit_2014) as profit_2014, " +
                        agg + "(remission_2018) as remission_2018, " + agg + "(remission_2017) as remission_2017, " +
                        agg + "(remission_2016) as remission_2016, " + agg + "(remission_2015) as remission_2015, " + agg + "(remission_2014) as remission_2014, industrial_type from listed_company";
                if (province != null || city != null) {
                    sqlSentence += " where industrial_type is not null";
                    if (province != null) {
                        sqlSentence += " and province = '" + province + "'";
                    }
                    if (city != null) {
                        sqlSentence += " and city = '" + city + "'";
                    }
                }
                sqlSentence += " group by industrial_type order by income_2018 desc limit 3";
                ResultSet resultSet = getResultSet(sqlSentence);
                while (resultSet.next()) {
                    ObjectNode typeObjectNode = objectMapper.createObjectNode();
                    ObjectNode tempObjectNode = objectMapper.createObjectNode();
                    tempObjectNode.put("2018", resultSet.getDouble("income_2018"));
                    tempObjectNode.put("2017", resultSet.getDouble("income_2017"));
                    tempObjectNode.put("2016", resultSet.getDouble("income_2016"));
                    tempObjectNode.put("2015", resultSet.getDouble("income_2015"));
                    tempObjectNode.put("2014", resultSet.getDouble("income_2014"));
                    typeObjectNode.set("income", tempObjectNode);
                    tempObjectNode = objectMapper.createObjectNode();
                    tempObjectNode.put("2018", resultSet.getDouble("rsh_cost_2018"));
                    tempObjectNode.put("2017", resultSet.getDouble("rsh_cost_2017"));
                    tempObjectNode.put("2016", resultSet.getDouble("rsh_cost_2016"));
                    tempObjectNode.put("2015", resultSet.getDouble("rsh_cost_2015"));
                    tempObjectNode.put("2014", resultSet.getDouble("rsh_cost_2014"));
                    typeObjectNode.set("rsh_cost", tempObjectNode);
                    tempObjectNode = objectMapper.createObjectNode();
                    tempObjectNode.put("2018", resultSet.getDouble("gov_sub_2018"));
                    tempObjectNode.put("2017", resultSet.getDouble("gov_sub_2017"));
                    tempObjectNode.put("2016", resultSet.getDouble("gov_sub_2016"));
                    tempObjectNode.put("2015", resultSet.getDouble("gov_sub_2015"));
                    tempObjectNode.put("2014", resultSet.getDouble("gov_sub_2014"));
                    typeObjectNode.set("gov_sub", tempObjectNode);
                    tempObjectNode = objectMapper.createObjectNode();
                    tempObjectNode.put("2018", resultSet.getDouble("tax_2018"));
                    tempObjectNode.put("2017", resultSet.getDouble("tax_2017"));
                    tempObjectNode.put("2016", resultSet.getDouble("tax_2016"));
                    tempObjectNode.put("2015", resultSet.getDouble("tax_2015"));
                    tempObjectNode.put("2014", resultSet.getDouble("tax_2014"));
                    typeObjectNode.set("tax", tempObjectNode);
                    tempObjectNode = objectMapper.createObjectNode();
                    tempObjectNode.put("2018", resultSet.getDouble("profit_2018"));
                    tempObjectNode.put("2017", resultSet.getDouble("profit_2017"));
                    tempObjectNode.put("2016", resultSet.getDouble("profit_2016"));
                    tempObjectNode.put("2015", resultSet.getDouble("profit_2015"));
                    tempObjectNode.put("2014", resultSet.getDouble("profit_2014"));
                    typeObjectNode.set("profit", tempObjectNode);
                    tempObjectNode = objectMapper.createObjectNode();
                    tempObjectNode.put("2018", resultSet.getDouble("remission_2018"));
                    tempObjectNode.put("2017", resultSet.getDouble("remission_2017"));
                    tempObjectNode.put("2016", resultSet.getDouble("remission_2016"));
                    tempObjectNode.put("2015", resultSet.getDouble("remission_2015"));
                    tempObjectNode.put("2014", resultSet.getDouble("remission_2014"));
                    typeObjectNode.set("remission", tempObjectNode);
                    objectNode.set(resultSet.getString("industrial_type"), typeObjectNode);
                }
            } else {
                String sqlSentence = "select " + agg + "(income_2018) as income_2018, " + agg + "(income_2017) as income_2017, " +
                        agg + "(income_2016) as income_2016, " + agg + "(income_2015) as income_2015, " + agg + "(income_2014) as income_2014, " +
                        agg + "(rsh_cost_2018) as rsh_cost_2018, " + agg + "(rsh_cost_2017) as rsh_cost_2017, " +
                        agg + "(rsh_cost_2016) as rsh_cost_2016, " + agg + "(rsh_cost_2015) as rsh_cost_2015, " + agg + "(rsh_cost_2014) as rsh_cost_2014, " +
                        agg + "(gov_sub_2018) as gov_sub_2018, " + agg + "(gov_sub_2017) as gov_sub_2017, " +
                        agg + "(gov_sub_2016) as gov_sub_2016, " + agg + "(gov_sub_2015) as gov_sub_2015, " + agg + "(gov_sub_2014) as gov_sub_2014, " +
                        agg + "(tax_2018) as tax_2018, " + agg + "(tax_2017) as tax_2017, " +
                        agg + "(tax_2016) as tax_2016, " + agg + "(tax_2015) as tax_2015, " + agg + "(tax_2014) as tax_2014, " +
                        agg + "(profit_2018) as profit_2018, " + agg + "(profit_2017) as profit_2017, " +
                        agg + "(profit_2016) as profit_2016, " + agg + "(profit_2015) as profit_2015, " + agg + "(profit_2014) as profit_2014, " +
                        agg + "(remission_2018) as remission_2018, " + agg + "(remission_2017) as remission_2017, " +
                        agg + "(remission_2016) as remission_2016, " + agg + "(remission_2015) as remission_2015, " + agg + "(remission_2014) as remission_2014 from listed_company";
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
                ResultSet resultSet = getResultSet(sqlSentence);
                if (resultSet.next()) {
                    ObjectNode tempObjectNode = objectMapper.createObjectNode();
                    tempObjectNode.put("2018", resultSet.getDouble("income_2018"));
                    tempObjectNode.put("2017", resultSet.getDouble("income_2017"));
                    tempObjectNode.put("2016", resultSet.getDouble("income_2016"));
                    tempObjectNode.put("2015", resultSet.getDouble("income_2015"));
                    tempObjectNode.put("2014", resultSet.getDouble("income_2014"));
                    objectNode.set("income", tempObjectNode);
                    tempObjectNode = objectMapper.createObjectNode();
                    tempObjectNode.put("2018", resultSet.getDouble("rsh_cost_2018"));
                    tempObjectNode.put("2017", resultSet.getDouble("rsh_cost_2017"));
                    tempObjectNode.put("2016", resultSet.getDouble("rsh_cost_2016"));
                    tempObjectNode.put("2015", resultSet.getDouble("rsh_cost_2015"));
                    tempObjectNode.put("2014", resultSet.getDouble("rsh_cost_2014"));
                    objectNode.set("rsh_cost", tempObjectNode);
                    tempObjectNode = objectMapper.createObjectNode();
                    tempObjectNode.put("2018", resultSet.getDouble("gov_sub_2018"));
                    tempObjectNode.put("2017", resultSet.getDouble("gov_sub_2017"));
                    tempObjectNode.put("2016", resultSet.getDouble("gov_sub_2016"));
                    tempObjectNode.put("2015", resultSet.getDouble("gov_sub_2015"));
                    tempObjectNode.put("2014", resultSet.getDouble("gov_sub_2014"));
                    objectNode.set("gov_sub", tempObjectNode);
                    tempObjectNode = objectMapper.createObjectNode();
                    tempObjectNode.put("2018", resultSet.getDouble("tax_2018"));
                    tempObjectNode.put("2017", resultSet.getDouble("tax_2017"));
                    tempObjectNode.put("2016", resultSet.getDouble("tax_2016"));
                    tempObjectNode.put("2015", resultSet.getDouble("tax_2015"));
                    tempObjectNode.put("2014", resultSet.getDouble("tax_2014"));
                    objectNode.set("tax", tempObjectNode);
                    tempObjectNode = objectMapper.createObjectNode();
                    tempObjectNode.put("2018", resultSet.getDouble("profit_2018"));
                    tempObjectNode.put("2017", resultSet.getDouble("profit_2017"));
                    tempObjectNode.put("2016", resultSet.getDouble("profit_2016"));
                    tempObjectNode.put("2015", resultSet.getDouble("profit_2015"));
                    tempObjectNode.put("2014", resultSet.getDouble("profit_2014"));
                    objectNode.set("profit", tempObjectNode);
                    tempObjectNode = objectMapper.createObjectNode();
                    tempObjectNode.put("2018", resultSet.getDouble("remission_2018"));
                    tempObjectNode.put("2017", resultSet.getDouble("remission_2017"));
                    tempObjectNode.put("2016", resultSet.getDouble("remission_2016"));
                    tempObjectNode.put("2015", resultSet.getDouble("remission_2015"));
                    tempObjectNode.put("2014", resultSet.getDouble("remission_2014"));
                    objectNode.set("remission", tempObjectNode);
                }
            }
            log.printExecuteOkInfo(httpServletRequest);
            return objectNode;
        } catch (InterruptedException | ExecutionException | TimeoutException | SQLException | NullPointerException | IllegalParameterException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }

    @GetMapping("/all")
    public ObjectNode queryAll(HttpServletRequest httpServletRequest,
                               @RequestParam(required = false, value = "limit") String limit,
                               @RequestParam(required = false, value = "offset") String offset) {
        try {
            ObjectNode objectNode = objectMapper.createObjectNode();
            String sqlSentence;
            if (limit == null || offset == null) {
                sqlSentence = "select * from listed_company order by id asc";
            } else {
                sqlSentence = "select id, com_chn_name, (bachelor_num + master_num + doctor_num) as employee_num, address, " +
                        "(income_2018 + income_2017 + income_2016 + income_2015 + income_2014) as income from listed_company order by id asc limit " + limit + " offset " + offset;
            }
            ResultSet resultSet = getResultSet(sqlSentence);
            ArrayNode arrayNode = objectMapper.createArrayNode();
            while (resultSet.next()) {
                ObjectNode tempObjectNode = objectMapper.createObjectNode();
                tempObjectNode.put("id", resultSet.getInt("id"));
                tempObjectNode.put("name", resultSet.getString("com_chn_name"));
                tempObjectNode.put("employee", resultSet.getInt("employee_num"));
                tempObjectNode.put("address", resultSet.getString("address"));
                tempObjectNode.put("income", resultSet.getDouble("income"));
                arrayNode.add(tempObjectNode);
            }
            objectNode.set("all" ,arrayNode);
            log.printExecuteOkInfo(httpServletRequest);
            return objectNode;
        } catch (InterruptedException | ExecutionException | TimeoutException | SQLException | NullPointerException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }
}
