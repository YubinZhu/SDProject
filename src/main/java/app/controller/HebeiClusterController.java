package app.controller;

import app.exception.IllegalParameterException;
import app.service.LogService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static app.controller.GeometryController.multiPolygonStringToArrayNode;
import static app.service.DatabaseService.getResultSet;

/**
 * Created by yubzhu on 19-8-19
 */

@RestController
@CrossOrigin(origins = "*")
@RequestMapping("/hebei")
public class HebeiClusterController {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final LogService log = new LogService(HebeiClusterController.class);

    @GetMapping("/category")
    public ObjectNode queryCategory(HttpServletRequest httpServletRequest) {
        try {
            ObjectNode objectNode = objectMapper.createObjectNode();
            String sqlSentence = "select distinct(industrial_type) from hebei_cluster where industrial_type is not null order by industrial_type asc";
            ResultSet resultSet = getResultSet(sqlSentence);
            ArrayNode arrayNode = objectMapper.createArrayNode();
            while (resultSet.next()) {
                arrayNode.add(resultSet.getString("industrial_type"));
            }
            objectNode.set("type", arrayNode);
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
            String sqlSentence = "select cluster_name from hebei_cluster where cluster_name ~* '" + name + "' order by id asc limit 20";
            ResultSet resultSet = getResultSet(sqlSentence);
            ArrayNode arrayNode = objectMapper.createArrayNode();
            while (resultSet.next()) {
                arrayNode.add(resultSet.getString("cluster_name"));
            }
            objectNode.set("cluster", arrayNode);
            log.printExecuteOkInfo(httpServletRequest);
            return objectNode;
        } catch (InterruptedException | ExecutionException | TimeoutException | SQLException | NullPointerException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }

    @GetMapping("/information")
    public ObjectNode queryGeometry(HttpServletRequest httpServletRequest,
                                    @RequestParam(value = "name") String name) {
        try {
            ObjectNode objectNode = objectMapper.createObjectNode();
            String sqlSentence = "select * from hebei_cluster where cluster_name = '" + name + "' order by id asc";
            ResultSet resultSet = getResultSet(sqlSentence);
            if (resultSet.next()) {
                objectNode.put("city", resultSet.getString("city"));
                objectNode.put("name", resultSet.getString("cluster_name"));
                objectNode.put("district", resultSet.getString("district"));
                objectNode.put("type", resultSet.getString("industrial_type"));
                objectNode.put("production", resultSet.getString("production"));
                objectNode.put("produce_company_num", resultSet.getInt("produce_company_num"));
                objectNode.put("matched_company_num", resultSet.getInt("matched_company_num"));
                objectNode.put("related_company_num", resultSet.getInt("related_company_num"));
                objectNode.put("income", resultSet.getInt("income"));
                String tempSqlSentence = "select st_astext(geom) from district_boundary where province = '河北省' and city = '" +
                        resultSet.getString("city") + "' and district = '" + resultSet.getString("district") + "'";
                ResultSet tempResultSet = getResultSet(tempSqlSentence);
                if (tempResultSet.next()) {
                    objectNode.set("multipolygon", multiPolygonStringToArrayNode(tempResultSet.getString("st_astext")));
                } else {
                    objectNode.set("multipolygon", null);
                }
            }
            log.printExecuteOkInfo(httpServletRequest);
            return objectNode;
        } catch (InterruptedException | ExecutionException | TimeoutException | SQLException | IOException | IllegalParameterException | NullPointerException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }

    @GetMapping("/statistic")
    public ObjectNode queryStatistic(HttpServletRequest httpServletRequest,
                                     @RequestParam(value = "type") String type) {
        try {
            ObjectNode objectNode = objectMapper.createObjectNode();
            String sqlSentence = "select * from hebei_cluster where industrial_type = '" + type + "' order by id asc";
            ResultSet resultSet = getResultSet(sqlSentence);
            int produceCompanyNum = 0;
            int matchedCompanyNum = 0;
            int relatedCompanyNum = 0;
            int income = 0;
            ArrayNode clusterArrayNode = objectMapper.createArrayNode();
            ArrayNode multiPolygonArrayNode = objectMapper.createArrayNode();
            while (resultSet.next()) {
                produceCompanyNum += resultSet.getInt("produce_company_num");
                matchedCompanyNum += resultSet.getInt("matched_company_num");
                relatedCompanyNum += resultSet.getInt("related_company_num");
                income += resultSet.getInt("income");
                clusterArrayNode.add(resultSet.getString("cluster_name"));
                String tempSqlSentence = "select st_astext(geom) from district_boundary where province = '河北省' and city = '" +
                        resultSet.getString("city") + "' and district = '" + resultSet.getString("district") + "'";
                ResultSet tempResultSet = getResultSet(tempSqlSentence);
                if (tempResultSet.next()) {
                    ArrayNode insideArrayNode = multiPolygonStringToArrayNode(tempResultSet.getString("st_astext"));
                    for (int i = 0; i < insideArrayNode.size(); i += 1) {
                        multiPolygonArrayNode.add(insideArrayNode.get(i));
                    }
                } else {
                    System.out.println(resultSet.getString("district"));
                    tempSqlSentence = "select st_astext(geom) from district_boundary where province = '河北省' and city = '" + resultSet.getString("district") + "'";
                    tempResultSet = getResultSet(tempSqlSentence);
                    if (tempResultSet.next()) {
                        ArrayNode insideArrayNode = multiPolygonStringToArrayNode(tempResultSet.getString("st_astext"));
                        for (int i = 0; i < insideArrayNode.size(); i += 1) {
                            multiPolygonArrayNode.add(insideArrayNode.get(i));
                        }
                    }
                }
            }
            objectNode.put("produce_company_num", produceCompanyNum);
            objectNode.put("matched_company_num", matchedCompanyNum);
            objectNode.put("related_company_num", relatedCompanyNum);
            objectNode.put("income", income);
            objectNode.set("cluster", clusterArrayNode);
            objectNode.set("multipolygon", multiPolygonArrayNode);
            log.printExecuteOkInfo(httpServletRequest);
            return objectNode;
        } catch (InterruptedException | ExecutionException | TimeoutException | SQLException | IOException | IllegalParameterException | NullPointerException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }
}
