package app.controller;

import app.service.LogService;
import app.service.QueryTableService;
import com.fasterxml.jackson.databind.ObjectMapper;
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
@RequestMapping("/information")
public class InformationController {

    @Autowired
    private static ObjectMapper objectMapper = new ObjectMapper();

    private static LogService log = new LogService(InformationController.class);

    @GetMapping("/qingdao")
    public ObjectNode queryCompanyInformation(HttpServletRequest httpServletRequest,
                                              @RequestParam(value = "id") String id) {
        ObjectNode objectNode = objectMapper.createObjectNode();
        try {
            String sqlSentence = "select * from ent_info where ent_id=" + id;
            ResultSet resultSet = QueryTableService.query(sqlSentence);
            if (resultSet.next()) {
                objectNode.put("id", resultSet.getInt("ent_id"));
                objectNode.put("name", resultSet.getString("ent_name"));
                objectNode.put("label", resultSet.getString("ent_label"));
                objectNode.put("industry", resultSet.getString("ent_industry"));
                objectNode.put("lon", resultSet.getDouble("lon"));
                objectNode.put("lat", resultSet.getDouble("lat"));
            }
            log.printQueryOkInfo(httpServletRequest);
        } catch (ClassNotFoundException | SQLException e) {
            log.printExceptionOccurredWarning(httpServletRequest, e);
            objectNode.removeAll();
            objectNode.put("exception", e.getClass().getSimpleName());
        }
        return objectNode;
    }
}
