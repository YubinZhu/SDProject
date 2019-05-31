package app.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import app.service.QueryTableService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.*;
import java.util.HashMap;

/**
 * Created by yubzhu on 19-4-30
 */

@RestController
@CrossOrigin(origins = "*")
@RequestMapping("/demo")
public class InfoQueryController {

    /* template
        ObjectNode objectNode = jacksonObjectMapper.createObjectNode();
        try {
            ResultSet resultSet = QueryTableService.query("");

            // logical code here

            printQueryOkInfo(httpServletRequest);
        } catch (ClassNotFoundException | SQLException e) {
            printExceptionOccurredWarning(httpServletRequest, e);
            objectNode.removeAll();
            objectNode.put("exception", e.getClass().getSimpleName());
        }
        return objectNode;
    */

    @Autowired
    private ObjectMapper jacksonObjectMapper = new ObjectMapper();

    private static Logger logger = LoggerFactory.getLogger(InfoQueryController.class);

    private static String getRequestURL(HttpServletRequest httpServletRequest) throws UnsupportedEncodingException {
        if (httpServletRequest.getQueryString() == null) {
            return httpServletRequest.getRequestURL().toString();
        } else {
            return httpServletRequest.getRequestURL() + "?" + URLDecoder.decode(httpServletRequest.getQueryString(), "utf-8");
        }
    }

    private static void printQueryOkInfo(HttpServletRequest httpServletRequest) {
        try {
            String url = getRequestURL(httpServletRequest);
            logger.info("At [{}] --- Query ok.", url);
        } catch (UnsupportedEncodingException e) {
            logger.error("FATAL ERROR --- Exception \"{}\" occurred.", UnsupportedEncodingException.class.getName());
        }
    }

    private static void printExceptionOccurredWarning(HttpServletRequest httpServletRequest, Exception exception) {
        try {
            String url = getRequestURL(httpServletRequest);
            logger.warn("At [{}] --- Exception \"{}\" occurred.", url, exception.getClass().getName());
        } catch (UnsupportedEncodingException e) {
            logger.error("FATAL ERROR --- Exception \"{}\" occurred.", UnsupportedEncodingException.class.getName());
        }
    }

    // todo: delete hard code
    @GetMapping("/queryCompanyCoordinatesByCategory")
    public ObjectNode queryCompanyCoordinatesByCategory(HttpServletRequest httpServletRequest,
                                                        @RequestParam(required = false, value = "category") String queriedCategory) {
        /* hard code begin */
        HashMap<String, Integer> hardCodeHashMap = new HashMap<>();
        hardCodeHashMap.put("科技", 1);
        hardCodeHashMap.put("机械", 2);
        hardCodeHashMap.put("食品", 3);
        hardCodeHashMap.put("其他", 4);
        /* hard code end */

        ObjectNode objectNode = jacksonObjectMapper.createObjectNode();
        try {
            objectNode.put("type", "FeatureCollection");
            ArrayNode arrayNode = jacksonObjectMapper.createArrayNode();
            ResultSet resultSet;
            if (queriedCategory == null) {
                resultSet = QueryTableService.query("select id, lon, lat, category from company_info");
            } else {
                resultSet = QueryTableService.query("select id, lon, lat, category from company_info where category = '" + queriedCategory + "'");
            }
            // HashMap<String, Integer> hashMap = new HashMap<>();
            HashMap<String, Integer> hashMap = hardCodeHashMap; // hard code
            while (resultSet.next()) {
                hashMap.putIfAbsent(resultSet.getString("category"), hashMap.keySet().size() + 1);
                ObjectNode tempObjectNode = jacksonObjectMapper.createObjectNode();
                tempObjectNode.put("type", "Feature");
                tempObjectNode.put("id", resultSet.getInt("id"));
                ObjectNode insideObjectNode = jacksonObjectMapper.createObjectNode();
                insideObjectNode.put("category", hashMap.get(resultSet.getString("category")));
                tempObjectNode.set("properties", insideObjectNode);
                insideObjectNode = jacksonObjectMapper.createObjectNode();
                insideObjectNode.put("type", "Point");
                ArrayNode insideArrayNode = jacksonObjectMapper.createArrayNode();
                insideArrayNode.add(resultSet.getDouble("lon"));
                insideArrayNode.add(resultSet.getDouble("lat"));
                insideObjectNode.set("coordinates", insideArrayNode);
                tempObjectNode.set("geometry", insideObjectNode);
                arrayNode.add(tempObjectNode);
            }
            objectNode.set("features", arrayNode);
            ObjectNode tempObjectNode = jacksonObjectMapper.createObjectNode();
            for (String string : hashMap.keySet()) {
                tempObjectNode.put(String.valueOf(hashMap.get(string)), string);
            }
            objectNode.set("categoryMapping", tempObjectNode);
            printQueryOkInfo(httpServletRequest);
        } catch (ClassNotFoundException | SQLException e) {
            printExceptionOccurredWarning(httpServletRequest, e);
            objectNode.removeAll();
            objectNode.put("exception", e.getClass().getSimpleName());
        }
        return objectNode;
    }

    @GetMapping("/queryCompanyListByName")
    public ObjectNode queryCompanyListByName(HttpServletRequest httpServletRequest,
                                             @RequestParam(value = "name") String queriedName) {
        ObjectNode objectNode = jacksonObjectMapper.createObjectNode();
        try {
            ResultSet resultSet = QueryTableService.query("select id, name from company_info where name ~* '" + queriedName + "'");
            ArrayNode arrayNode = jacksonObjectMapper.createArrayNode();
            while(resultSet.next()) {
                ObjectNode tempObjectNode = jacksonObjectMapper.createObjectNode();
                tempObjectNode.put("id", resultSet.getInt("id"));
                tempObjectNode.put("name", resultSet.getString("name"));
                arrayNode.add(tempObjectNode);
            }
            objectNode.set("company", arrayNode);
            printQueryOkInfo(httpServletRequest);
        } catch (ClassNotFoundException | SQLException e) {
            printExceptionOccurredWarning(httpServletRequest, e);
            objectNode.removeAll();
            objectNode.put("exception", e.getClass().getSimpleName());
        }
        return objectNode;
    }

    // todo : delete redundant code
    @GetMapping("/queryCompanyInformationByName")
    public ObjectNode queryCompanyInformationByName(HttpServletRequest httpServletRequest,
                                                    @RequestParam(value = "name") String queriedName) {
        ObjectNode objectNode = jacksonObjectMapper.createObjectNode();
        try {
            //ResultSet resultSet = QueryTableService.query("select id, name from company_info where name ~* '" + queriedName + "'");
            ResultSet resultSet = QueryTableService.query("select * from company_info where name ~* '" + queriedName + "'");
            if (resultSet.next()) {
                /*objectNode.put("id", resultSet.getInt("id"));
                objectNode.put("name", resultSet.getString("name"));*/
                objectNode.put("id", resultSet.getInt("id"));
                objectNode.put("name", resultSet.getString("name"));
                objectNode.put("lon", resultSet.getDouble("lon"));
                objectNode.put("lat", resultSet.getDouble("lat"));
                objectNode.put("category", resultSet.getString("category"));
                objectNode.put("employee", resultSet.getInt("employee"));
                objectNode.put("profit", resultSet.getInt("profit"));
                objectNode.put("investment", resultSet.getInt("investment"));
                objectNode.put("electricity", resultSet.getInt("electricity"));
                objectNode.put("researchers", resultSet.getDouble("researchers"));
            }
            printQueryOkInfo(httpServletRequest);
        } catch (ClassNotFoundException | SQLException e) {
            printExceptionOccurredWarning(httpServletRequest, e);
            objectNode.removeAll();
            objectNode.put("exception", e.getClass().getSimpleName());
        }
        return objectNode;
    }

    @GetMapping("/queryCompanyInformationById")
    public ObjectNode queryCompanyInformationById(HttpServletRequest httpServletRequest,
                                                  @RequestParam(value = "id") int queriedId) {
        ObjectNode objectNode = jacksonObjectMapper.createObjectNode();
        try {
            ResultSet resultSet = QueryTableService.query("select * from company_info where id = " + queriedId);
            if (resultSet.next()) {
                objectNode.put("id", resultSet.getInt("id"));
                objectNode.put("name", resultSet.getString("name"));
                objectNode.put("lon", resultSet.getDouble("lon"));
                objectNode.put("lat", resultSet.getDouble("lat"));
                objectNode.put("category", resultSet.getString("category"));
                objectNode.put("employee", resultSet.getInt("employee"));
                objectNode.put("profit", resultSet.getInt("profit"));
                objectNode.put("investment", resultSet.getInt("investment"));
                objectNode.put("electricity", resultSet.getInt("electricity"));
                objectNode.put("researchers", resultSet.getDouble("researchers"));
            }
            printQueryOkInfo(httpServletRequest);
        } catch (ClassNotFoundException | SQLException e) {
            printExceptionOccurredWarning(httpServletRequest, e);
            objectNode.removeAll();
            objectNode.put("exception", e.getClass().getSimpleName());
        }
        return objectNode;
    }

    @GetMapping("/queryParkListByName")
    public ObjectNode queryParkListByName(HttpServletRequest httpServletRequest,
                                          @RequestParam(value = "name") String queriedName) {
        ObjectNode objectNode = jacksonObjectMapper.createObjectNode();
        try {
            ResultSet resultSet = QueryTableService.query("select id, name from park_info where name ~* '" + queriedName + "'");
            ArrayNode arrayNode = jacksonObjectMapper.createArrayNode();
            while(resultSet.next()) {
                ObjectNode tempObjectNode = jacksonObjectMapper.createObjectNode();
                tempObjectNode.put("id", resultSet.getInt("id"));
                tempObjectNode.put("name", resultSet.getString("name"));
                arrayNode.add(tempObjectNode);
            }
            objectNode.set("park", arrayNode);
            printQueryOkInfo(httpServletRequest);
        } catch (ClassNotFoundException | SQLException e) {
            printExceptionOccurredWarning(httpServletRequest, e);
            objectNode.removeAll();
            objectNode.put("exception", e.getClass().getSimpleName());
        }
        return objectNode;
    }

    // todo : delete redundant code
    @GetMapping("/queryParkInformationByName")
    public ObjectNode queryParkInformationByName(HttpServletRequest httpServletRequest,
                                                 @RequestParam(value = "name") String queriedName) {
        ObjectNode objectNode = jacksonObjectMapper.createObjectNode();
        try {
            //ResultSet resultSet = QueryTableService.query("select id, name from park_info where name ~* '" + queriedName + "'");
            ResultSet resultSet = QueryTableService.query("select * from park_info where name ~* '" + queriedName + "'");
            if (resultSet.next()) {
                /*objectNode.put("id", resultSet.getInt("id"));
                objectNode.put("name", resultSet.getString("name"));*/
                objectNode.put("id", resultSet.getInt("id"));
                objectNode.put("name", resultSet.getString("name"));
                objectNode.put("lon", resultSet.getDouble("lon"));
                objectNode.put("lat", resultSet.getDouble("lat"));
                objectNode.put("description", resultSet.getString("description"));
            }
            printQueryOkInfo(httpServletRequest);
        } catch (ClassNotFoundException | SQLException e) {
            printExceptionOccurredWarning(httpServletRequest, e);
            objectNode.removeAll();
            objectNode.put("exception", e.getClass().getSimpleName());
        }
        return objectNode;
    }

    @GetMapping("/queryParkInformationById")
    public ObjectNode queryParkInformationById(HttpServletRequest httpServletRequest,
                                               @RequestParam(value = "id") int queriedId) {
        ObjectNode objectNode = jacksonObjectMapper.createObjectNode();
        try {
            ResultSet resultSet = QueryTableService.query("select * from park_info where id = " + queriedId);
            if (resultSet.next()) {
                objectNode.put("id", resultSet.getInt("id"));
                objectNode.put("name", resultSet.getString("name"));
                objectNode.put("lon", resultSet.getDouble("lon"));
                objectNode.put("lat", resultSet.getDouble("lat"));
                objectNode.put("description", resultSet.getString("description"));
            }
            printQueryOkInfo(httpServletRequest);
        } catch (ClassNotFoundException | SQLException e) {
            printExceptionOccurredWarning(httpServletRequest, e);
            objectNode.removeAll();
            objectNode.put("exception", e.getClass().getSimpleName());
        }
        return objectNode;
    }

    @GetMapping("/queryLandInformation")
    public ObjectNode queryLandInformation(HttpServletRequest httpServletRequest) {
        ObjectNode objectNode = jacksonObjectMapper.createObjectNode();
        try {
            HashMap<String, Integer> hashMap = new HashMap<>();
            objectNode.put("type", "FeatureCollection");
            ArrayNode arrayNode = jacksonObjectMapper.createArrayNode();
            ResultSet resultSet = QueryTableService.query("select * from land_info");
            while (resultSet.next()) {
                hashMap.putIfAbsent(resultSet.getString("category"), hashMap.keySet().size() + 1);
                ObjectNode tempObjectNode = jacksonObjectMapper.createObjectNode();
                tempObjectNode.put("type", "Feature");
                tempObjectNode.put("id", resultSet.getInt("id"));
                ObjectNode insideObjectNode = jacksonObjectMapper.createObjectNode();
                insideObjectNode.put("category", hashMap.get(resultSet.getString("category")));
                tempObjectNode.set("properties", insideObjectNode);
                insideObjectNode = jacksonObjectMapper.createObjectNode();
                insideObjectNode.put("type", "Polygon");
                ArrayNode insideArrayNode = jacksonObjectMapper.createArrayNode();
                insideArrayNode.add(jacksonObjectMapper.readTree(resultSet.getString("points")));
                insideObjectNode.set("coordinates", insideArrayNode);
                tempObjectNode.set("geometry", insideObjectNode);
                arrayNode.add(tempObjectNode);
            }
            objectNode.set("features", arrayNode);
            ObjectNode tempObjectNode = jacksonObjectMapper.createObjectNode();
            for (String string : hashMap.keySet()) {
                tempObjectNode.put(String.valueOf(hashMap.get(string)), string);
            }
            objectNode.set("categoryMapping", tempObjectNode);
            printQueryOkInfo(httpServletRequest);
        } catch (ClassNotFoundException | SQLException | IOException e) {
            printExceptionOccurredWarning(httpServletRequest, e);
            objectNode.removeAll();
            objectNode.put("exception", e.getClass().getSimpleName());
        }
        return objectNode;
    }

    // todo : complete
    // @GetMapping("/queryAggregateCompanyInformationByCategory")
    public ObjectNode queryAggregateCompanyInformationByCategory(HttpServletRequest httpServletRequest,
                                                                 @RequestParam(required = false, value = "name") String queriedCategory) {
        ObjectNode objectNode = jacksonObjectMapper.createObjectNode();
        try {
            ResultSet resultSet = QueryTableService.query("");
            printQueryOkInfo(httpServletRequest);
        } catch (ClassNotFoundException | SQLException e) {
            printExceptionOccurredWarning(httpServletRequest, e);
            objectNode.removeAll();
            objectNode.put("exception", e.getClass().getSimpleName());
        }
        return objectNode;
    }

    // todo ： different park has different companies.
    // @GetMapping("queryAggregateParkInformationById")
    public ObjectNode queryAggregateParkInformationById(HttpServletRequest httpServletRequest,
                                                        @RequestParam(value = "id") int queriedId) {
        ObjectNode objectNode = jacksonObjectMapper.createObjectNode();
        try {
            ResultSet resultSetTotal = QueryTableService.query("select count(*) as company_num, sum(employee) as employee_sum, sum(profit) as profit_sum, sum(investment) as investment_sum, sum(electricity) as electricity_sum from company_info");
            ResultSet resultSet = QueryTableService.query("select count(*) as company_num, sum(employee) as employee_sum, sum(profit) as profit_sum, sum(investment) as investment_sum, sum(electricity) as electricity_sum, category from company_info group by category");
            ObjectNode companyNumObjectNode = jacksonObjectMapper.createObjectNode();
            ObjectNode employeeSumObjectNode = jacksonObjectMapper.createObjectNode();
            ObjectNode profitSumObjectNode = jacksonObjectMapper.createObjectNode();
            ObjectNode investmentSumObjectNode = jacksonObjectMapper.createObjectNode();
            ObjectNode electricitySumObjectNode = jacksonObjectMapper.createObjectNode();
            if (resultSetTotal.next()) {
                companyNumObjectNode.put("total", resultSetTotal.getInt("company_num"));
                employeeSumObjectNode.put("total", resultSetTotal.getInt("employee_sum"));
                profitSumObjectNode.put("total", resultSetTotal.getInt("profit_sum"));
                investmentSumObjectNode.put("total", resultSetTotal.getInt("investment_sum"));
                electricitySumObjectNode.put("total", resultSetTotal.getInt("electricity_sum"));
            }
            while (resultSet.next()) {
                companyNumObjectNode.put(resultSet.getString("category"), resultSet.getInt("company_num"));
                employeeSumObjectNode.put(resultSet.getString("category"), resultSet.getInt("employee_sum"));
                profitSumObjectNode.put(resultSet.getString("category"), resultSet.getInt("profit_sum"));
                investmentSumObjectNode.put(resultSet.getString("category"), resultSet.getInt("investment_sum"));
                electricitySumObjectNode.put(resultSet.getString("category"), resultSet.getInt("electricity_sum"));
            }
            objectNode.set("company", companyNumObjectNode);
            objectNode.set("employee", employeeSumObjectNode);
            objectNode.set("profit", profitSumObjectNode);
            objectNode.set("investment", investmentSumObjectNode);
            objectNode.set("electricity", electricitySumObjectNode);
            printQueryOkInfo(httpServletRequest);
        } catch (ClassNotFoundException | SQLException e) {
            printExceptionOccurredWarning(httpServletRequest, e);
            objectNode.removeAll();
            objectNode.put("exception", e.getClass().getSimpleName());
        }
        return objectNode;
    }

    // todo： queryheatmap mapping link should be change
    @GetMapping("/heatmap")
    public ObjectNode queryHeatmap(HttpServletRequest httpServletRequest) {
        ObjectNode objectNode = jacksonObjectMapper.createObjectNode();
        try {
            int precision = 4;
            HashMap<String, Integer> hashMap = new HashMap<>();
            ResultSet resultSet = QueryTableService.query("select * from company_point");
            while (resultSet.next()) {
                String lon = String.valueOf(resultSet.getDouble("lon"));
                String lat = String.valueOf(resultSet.getDouble("lat"));
                String index = lon.split("\\.")[0] + "." + lon.split("\\.")[1].substring(0, precision) + "," + lat.split("\\.")[0] + "." + lat.split("\\.")[1].substring(0, precision);
                hashMap.putIfAbsent(index, 0);
                hashMap.put(index, hashMap.get(index) + 1);
            }

            objectNode.put("type", "FeatureCollection");
            ArrayNode arrayNode = jacksonObjectMapper.createArrayNode();
            for (String string : hashMap.keySet()) {
                ObjectNode tempObjectNode = jacksonObjectMapper.createObjectNode();
                tempObjectNode.put("type", "Feature");
                tempObjectNode.put("weight", hashMap.get(string));
                ObjectNode insideObjectNode = jacksonObjectMapper.createObjectNode();
                insideObjectNode.put("type", "Point");
                ArrayNode insideArrayNode = jacksonObjectMapper.createArrayNode();
                insideArrayNode.add(Double.parseDouble(string.split(",")[0]));
                insideArrayNode.add(Double.parseDouble(string.split(",")[1]));
                insideObjectNode.set("coordinates", insideArrayNode);
                tempObjectNode.set("geometry", insideObjectNode);
                arrayNode.add(tempObjectNode);
            }
            objectNode.set("features", arrayNode);
            printQueryOkInfo(httpServletRequest);
        } catch (ClassNotFoundException | SQLException e) {
            printExceptionOccurredWarning(httpServletRequest, e);
            objectNode.removeAll();
            objectNode.put("exception", e.getClass().getSimpleName());
        }
        return objectNode;
    }

}
