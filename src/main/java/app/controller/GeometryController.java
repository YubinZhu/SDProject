package app.controller;

import app.exception.IllegalParameterException;
import app.service.AMapService;
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

import static app.service.DatabaseService.getResultSet;

/**
 * Created by yubzhu on 19-8-4
 */

@RestController
@CrossOrigin(origins = "*")
@RequestMapping("/geometry")
public class GeometryController {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final LogService log = new LogService(GeometryController.class);

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
            log.printExecuteOkInfo(httpServletRequest);
            return objectNode;
        } catch (NullPointerException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }

    @GetMapping("/default")
    public ObjectNode queryDefaultDistrictPolygon(HttpServletRequest httpServletRequest) {
        return queryDistrictPolygon(httpServletRequest, null, null, null, "city");
    }

    static ArrayNode multiPolygonStringToArrayNode(String multiPolygonString) throws IOException{
        return (ArrayNode)objectMapper.readTree(multiPolygonString.replace("MULTIPOLYGON(((", "[[[").replace(")))", "]]]").replace(")),((", "],[").replace(",", "],[").replace(" ", ", ").replace("],[", "], ["));
    }

    private ArrayNode packupPolygon(String sqlSentence, String level) throws InterruptedException, ExecutionException, TimeoutException, SQLException, IOException {
        ResultSet resultSet = getResultSet(sqlSentence);
        ArrayNode arrayNode = objectMapper.createArrayNode();
        while (resultSet.next()) {
            ObjectNode objectNode = objectMapper. createObjectNode();
            if (level.equals("country")) {
                objectNode.put("name", "中华人民共和国");
            } else {
                objectNode.put("name", resultSet.getString(level));
            }
            objectNode.set("multipolygon", multiPolygonStringToArrayNode(resultSet.getString("st_astext")));
            arrayNode.add(objectNode);
        }
        return arrayNode;
    }

    @GetMapping("/district")
    public ObjectNode queryDistrictPolygon(HttpServletRequest httpServletRequest,
                                           @RequestParam(required = false, value = "province") String province,
                                           @RequestParam(required = false, value = "city") String city,
                                           @RequestParam(required = false, value = "district") String district,
                                           @RequestParam(required = false, value = "level") String level) {
        try {
            ObjectNode objectNode = objectMapper.createObjectNode();
            if (level.equals("country") || level.equals("province") || level.equals("city") || level.equals("district")) {
                if (province == null) {
                    if (city != null || district != null) {
                        throw new IllegalParameterException();
                    }
                    objectNode.set("country", packupPolygon("select st_astext(geom) from district_boundary where province = '全部'", "country"));
                }
            } else {
                throw new IllegalParameterException();
            }
            if (level.equals("province") || level.equals("city") || level.equals("district")) {
                if (province == null) {
                    objectNode.set("province", packupPolygon("select province, st_astext(geom) from district_boundary where province != '全部' and city = '全部'", "province"));
                } else if (city == null) {
                    if (district != null) {
                        throw new IllegalParameterException();
                    }
                    objectNode.set("province", packupPolygon("select province, st_astext(geom) from district_boundary where province = '" + province + "' and city = '全部'", "province"));
                }
            }
            if (level.equals("city") || level.equals("district")) {
                if (city == null) {
                    if (province == null) {
                        objectNode.set("city", packupPolygon("select city, st_astext(geom) from district_boundary where city != '全部' and district = '全部'", "city"));
                    } else {
                        objectNode.set("city", packupPolygon("select city, st_astext(geom) from district_boundary where province = '" + province + "' and city != '全部' and district = '全部'", "city"));
                    }
                } else if (district == null) {
                    objectNode.set("city", packupPolygon("select city, st_astext(geom) from district_boundary where province = '" + province + "' and city = '" + city + "' and district = '全部'", "city"));
                }
            }
            if (level.equals("district")) {
                if (district == null) {
                    if (province == null) {
                        objectNode.set("district", packupPolygon("select district, st_astext(geom) from district_boundary where district != '全部'", "district"));
                    } else if (city == null) {
                        objectNode.set("district", packupPolygon("select district, st_astext(geom) from district_boundary where province = '" + province + "' and district != '全部'", "district"));
                    } else {
                        objectNode.set("district", packupPolygon("select district, st_astext(geom) from district_boundary where province = '" + province + "' and city = '" + city + "' and district != '全部'", "district"));
                    }
                } else {
                    objectNode.set("district", packupPolygon("select district, st_astext(geom) from district_boundary where province = '" + province + "' and city = '" + city + "' and district = '" + district + "'", "district"));
                }
            }
            /* double check for bad data */
            if (level.equals("city") || level.equals("district")) {
                if (city != null && objectNode.get("city").size() == 0) {
                    objectNode.set("city", packupPolygon("select district, st_astext(geom) from district_boundary where district = '" + city + "'", "district"));
                    if (objectNode.get("district") != null) {
                        objectNode.set("district", objectMapper.createArrayNode());
                    }
                }
            }
            log.printExecuteOkInfo(httpServletRequest);
            return objectNode;
        } catch (InterruptedException | ExecutionException | TimeoutException | SQLException | NullPointerException | IOException | IllegalParameterException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }

    @GetMapping("/relation")
    public ObjectNode queryRelation(HttpServletRequest httpServletRequest,
                                    @RequestParam(value = "keyword") String keyword) {
        try {
            ObjectNode result = AMapService.getDistrict(keyword, 1, "base");
            ObjectNode objectNode = objectMapper.createObjectNode();
            if (result == null) {
                throw new NullPointerException();
            }
            if (result.get("districts").get(0).get("level").asText().equals("district")) {
                return (ObjectNode)objectNode.set("subdistricts", objectMapper.createArrayNode());
            } else {
                ArrayNode arrayNode = (ArrayNode)result.get("districts").get(0).get("districts");
                ArrayNode tempArrayNode = objectMapper.createArrayNode();
                for (int i = 0; i < arrayNode.size(); i += 1) {
                    tempArrayNode.add(arrayNode.get(i).get("name").asText());
                }
                return (ObjectNode)objectNode.set("subdistricts", tempArrayNode);
            }
        } catch (NullPointerException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }

    @GetMapping("/customize")
    public ObjectNode updateCustomizedRegion(HttpServletRequest httpServletRequest) {
        // todo
        return null;
    }

}
