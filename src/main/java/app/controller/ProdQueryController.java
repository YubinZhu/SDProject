package app.controller;

import app.service.QueryTableService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by yubzhu on 19-5-30
 */

@RestController
@CrossOrigin(origins = "*")
@RequestMapping("/prod")
public class ProdQueryController {

    /* template
        ObjectNode objectNode = jacksonObjectMapper.createObjectNode();
        try {
            String sqlSentence = "";
            ResultSet resultSet = QueryTableService.query(sqlSentence);

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

    @GetMapping("/queryCompanyCategory")
    public ObjectNode queryCompanyCategory(HttpServletRequest httpServletRequest) {
        ObjectNode objectNode = jacksonObjectMapper.createObjectNode();
        try {
            String sqlSentence = "select distinct(ent_label) from ent_info where ent_label is not null order by ent_label asc";
            ResultSet resultSet = QueryTableService.query(sqlSentence);
            ArrayNode arrayNode = jacksonObjectMapper.createArrayNode();
            while (resultSet.next()) {
                arrayNode.add(resultSet.getString("ent_label"));
            }
            objectNode.set("ent_label", arrayNode);
            sqlSentence = "select distinct(ent_industry) from ent_info where ent_industry is not null order by ent_industry asc";
            resultSet = QueryTableService.query(sqlSentence);
            arrayNode = jacksonObjectMapper.createArrayNode();
            while (resultSet.next()) {
                arrayNode.add(resultSet.getString("ent_industry"));
            }
            objectNode.set("ent_industry", arrayNode);
            printQueryOkInfo(httpServletRequest);
        } catch (ClassNotFoundException | SQLException e) {
            printExceptionOccurredWarning(httpServletRequest, e);
            objectNode.removeAll();
            objectNode.put("exception", e.getClass().getSimpleName());
        }
        return objectNode;
    }

    @GetMapping("/queryCompanyCoordinates")
    public ObjectNode queryCompanyCoordinates(HttpServletRequest httpServletRequest,
                                              @RequestParam(required = false, value = "label") String label,
                                              @RequestParam(required = false, value = "industry") String industry) {
        ObjectNode objectNode = jacksonObjectMapper.createObjectNode();
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
            ArrayNode arrayNode = jacksonObjectMapper.createArrayNode();
            while (resultSet.next()) {
                ObjectNode featureObjectNode = jacksonObjectMapper.createObjectNode();
                featureObjectNode.put("type", "Feature");
                featureObjectNode.put("id", resultSet.getInt("ent_id"));
                featureObjectNode.put("industry", resultSet.getString("ent_industry"));
                ObjectNode geometryObjectNode = jacksonObjectMapper.createObjectNode();
                geometryObjectNode.put("type", "Point");
                ArrayNode coordArrayNode = jacksonObjectMapper.createArrayNode();
                coordArrayNode.add(resultSet.getDouble("lon"));
                coordArrayNode.add(resultSet.getDouble("lat"));
                geometryObjectNode.set("coordinates", coordArrayNode);
                featureObjectNode.set("geometry", geometryObjectNode);
                arrayNode.add(featureObjectNode);
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

    @GetMapping("/queryCompanyList")
    public ObjectNode queryCompanyList(HttpServletRequest httpServletRequest,
                                       @RequestParam(value = "name") String name) {
        ObjectNode objectNode = jacksonObjectMapper.createObjectNode();
        try {
            String sqlSentence = "select ent_id, ent_name from ent_info where ent_name ~* '" + name + "' order by ent_id asc";
            ResultSet resultSet = QueryTableService.query(sqlSentence);
            ArrayNode arrayNode = jacksonObjectMapper.createArrayNode();
            while (resultSet.next()) {
                ObjectNode tempObjectNode = jacksonObjectMapper.createObjectNode();
                tempObjectNode.put("id", resultSet.getInt("ent_id"));
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

    @GetMapping("/queryCompanyInformation")
    public ObjectNode queryCompanyInformation(HttpServletRequest httpServletRequest,
                                              @RequestParam(value = "id") String id) {
        ObjectNode objectNode = jacksonObjectMapper.createObjectNode();
        try {
            String sqlSentence = "select * from ent_info where id=" + id;
            ResultSet resultSet = QueryTableService.query(sqlSentence);
            if (resultSet.next()) {
                objectNode.put("id", resultSet.getInt("ent_id"));
                objectNode.put("name", resultSet.getString("ent_name"));
                objectNode.put("label", resultSet.getString("ent_label"));
                objectNode.put("industry", resultSet.getString("ent_industry"));
                objectNode.put("lon", resultSet.getDouble("lon"));
                objectNode.put("lat", resultSet.getDouble("lat"));
            }
            printQueryOkInfo(httpServletRequest);
        } catch (ClassNotFoundException | SQLException e) {
            printExceptionOccurredWarning(httpServletRequest, e);
            objectNode.removeAll();
            objectNode.put("exception", e.getClass().getSimpleName());
        }
        return objectNode;
    }

}
