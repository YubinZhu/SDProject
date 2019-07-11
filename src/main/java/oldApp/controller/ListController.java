package oldApp.controller;

import oldApp.service.LogService;
import oldApp.service.QueryTableService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by yubzhu on 2019/6/9
 */

@RestController
@CrossOrigin(origins = "*")
@RequestMapping("/list")
public class ListController {

    @Autowired
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final LogService log = new LogService(ListController.class);

    @GetMapping("/qingdao")
    public ObjectNode queryQingDaoCompanyList(HttpServletRequest httpServletRequest,
                                       @RequestParam(value = "name") String name) {
        ObjectNode objectNode = objectMapper.createObjectNode();
        try {
            String sqlSentence = "select ent_id, ent_name from ent_info where ent_name ~* '" + name + "' order by ent_id asc";
            ResultSet resultSet = QueryTableService.query(sqlSentence);
            ArrayNode arrayNode = objectMapper.createArrayNode();
            while (resultSet.next()) {
                ObjectNode tempObjectNode = objectMapper.createObjectNode();
                tempObjectNode.put("id", resultSet.getInt("ent_id"));
                tempObjectNode.put("name", resultSet.getString("ent_name"));
                arrayNode.add(tempObjectNode);
            }
            objectNode.set("company", arrayNode);
            log.printQueryOkInfo(httpServletRequest);
        } catch (ClassNotFoundException | SQLException e) {
            log.printExceptionOccurredWarning(httpServletRequest, e);
            objectNode.removeAll();
            objectNode.put("exception", e.getClass().getSimpleName());
        }
        return objectNode;
    }

    @GetMapping("/listed")
    public ObjectNode queryListedCompanyList(HttpServletRequest httpServletRequest,
                                             @RequestParam(value = "name") String name) {
        ObjectNode objectNode = objectMapper.createObjectNode();
        try {
            String sqlSentence = "select id, full_name from comp_info where full_name ~* '" + name + "' order by id asc";
            ResultSet resultSet = QueryTableService.query(sqlSentence);
            ArrayNode arrayNode = objectMapper.createArrayNode();
            while (resultSet.next()) {
                ObjectNode tempObjectNode = objectMapper.createObjectNode();
                tempObjectNode.put("id", resultSet.getInt("id"));
                tempObjectNode.put("name", resultSet.getString("full_name"));
                arrayNode.add(tempObjectNode);
            }
            objectNode.set("company", arrayNode);
            log.printQueryOkInfo(httpServletRequest);
        } catch (ClassNotFoundException | SQLException e) {
            log.printExceptionOccurredWarning(httpServletRequest, e);
            objectNode.removeAll();
            objectNode.put("exception", e.getClass().getSimpleName());
        }
        return objectNode;
    }

}
