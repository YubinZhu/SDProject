package app.controller;

import app.service.LogService;
import app.service.QueryTableService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by yubzhu on 2019/6/9
 */

@RestController
@CrossOrigin(origins = "*")
@RequestMapping("/category")
public class CategoryController {

    @Autowired
    private static ObjectMapper objectMapper = new ObjectMapper();

    private static LogService log = new LogService(CategoryController.class);

    @GetMapping("/qingdao")
    public ObjectNode queryCompanyCategory(HttpServletRequest httpServletRequest) {
        ObjectNode objectNode = objectMapper.createObjectNode();
        try {
            String sqlSentence = "select distinct(ent_label) from ent_info where ent_label is not null order by ent_label asc";
            ResultSet resultSet = QueryTableService.query(sqlSentence);
            ArrayNode arrayNode = objectMapper.createArrayNode();
            while (resultSet.next()) {
                arrayNode.add(resultSet.getString("ent_label"));
            }
            objectNode.set("ent_label", arrayNode);
            sqlSentence = "select distinct(ent_industry) from ent_info where ent_industry is not null order by ent_industry asc";
            resultSet = QueryTableService.query(sqlSentence);
            arrayNode = objectMapper.createArrayNode();
            while (resultSet.next()) {
                arrayNode.add(resultSet.getString("ent_industry"));
            }
            objectNode.set("ent_industry", arrayNode);
            log.printQueryOkInfo(httpServletRequest);
        } catch (ClassNotFoundException | SQLException e) {
            log.printExceptionOccurredWarning(httpServletRequest, e);
            objectNode.removeAll();
            objectNode.put("exception", e.getClass().getSimpleName());
        }
        return objectNode;
    }

}
