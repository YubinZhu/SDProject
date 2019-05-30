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
            String query = "";
            ResultSet resultSet = QueryTableService.query(query);

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
            String query = "select distinct(ent_label) from ent_info";
            ResultSet resultSet = QueryTableService.query(query);
            ArrayNode arrayNode = jacksonObjectMapper.createArrayNode();
            while (resultSet.next()) {
                arrayNode.add(resultSet.getString("ent_label"));
            }
            objectNode.set("ent_label", arrayNode);
            query = "select distinct(ent_industry) from ent_info";
            resultSet = QueryTableService.query(query);
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
                                              @RequestParam(required = false, value = "ent_label") String ent_label,
                                              @RequestParam(required = false, value = "ent_industry") String ent_industry) {
        ObjectNode objectNode = jacksonObjectMapper.createObjectNode();
        try {
            String query = "select ent_id, lon, lat from ent_info";
            if (ent_label != null && ent_industry != null) {
                query += " where ent_label = '" + ent_label + "' and ent_industry = '" + ent_industry + "'";
            } else if (ent_label != null) {
                query += " where ent_label = '" + ent_label + "'";
            } else if (ent_industry != null) {
                query += " where ent_industry = '" + ent_industry + "'";
            }
            ResultSet resultSet = QueryTableService.query(query);
            ArrayNode arrayNode = jacksonObjectMapper.createArrayNode();
            while (resultSet.next()) {
                ObjectNode featureObjectNode = jacksonObjectMapper.createObjectNode();
                featureObjectNode.put("type", "Feature");
                featureObjectNode.put(" id", resultSet.getInt("ent_id"));
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


}
