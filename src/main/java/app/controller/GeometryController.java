package app.controller;

import app.exception.IllegalParameterException;
import app.service.AMapService;
import app.service.LogService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static app.service.DatabaseService.getResultSet;
import static app.service.DatabaseService.update;

/**
 * Created by yubzhu on 19-8-4
 */

@RestController
@CrossOrigin(origins = "*")
@RequestMapping("/geometry")
public class GeometryController {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final LogService log = new LogService(GeometryController.class);

    static ArrayNode multiPolygonStringToArrayNode(String multiPolygonString) throws IOException {
        return (ArrayNode)objectMapper.readTree(multiPolygonString.replace("MULTIPOLYGON(((", "[[[").replace(")))", "]]]").replace(")),((", "],[").replace(",", "],[").replace(" ", ","));
    }

    private static String arrayNodeToMultiPolygonString(ArrayNode arrayNode) {
        return arrayNode.toString().replace("[[[", "MULTIPOLYGON(((").replace("]]]", ")))").replace(",", " ").replace("] [", ",").replace("],[", ")),((");
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

    @GetMapping("/save")
    public ObjectNode saveCustomizedRegion(HttpServletRequest httpServletRequest,
                                           @RequestParam(value = "geom") String geom,
                                           @RequestParam(value = "name") String name) {
        try {
            ObjectNode objectNode = objectMapper.createObjectNode();
            ResultSet resultSet = getResultSet("select count(*) from custom_region where name = '" + name + "'");
            resultSet.next();
            Integer integer;
            if (resultSet.getInt("count") != 0) {
                integer = update("update custom_region set geom = st_geomfromtext('" + arrayNodeToMultiPolygonString((ArrayNode)objectMapper.readTree(geom)) + "', 4326) where name = '" + name + "'");
                objectNode.put("message", "override");
            } else {
                integer = update("insert into custom_region(name, geom) values('" + name + "', st_geomfromtext('" + arrayNodeToMultiPolygonString((ArrayNode)objectMapper.readTree(geom)) + "', 4326))");
            }
            objectNode.put("result", integer.toString());
            log.printExecuteOkInfo(httpServletRequest);
            return objectNode;
        } catch (InterruptedException | ExecutionException | TimeoutException | SQLException | IOException | NullPointerException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }

    @GetMapping("/load")
    public ObjectNode loadCustomizedRegion(HttpServletRequest httpServletRequest,
                                           @RequestParam(value = "name") String name) {
        try {
            ObjectNode objectNode = objectMapper.createObjectNode();
            ResultSet resultSet = getResultSet("select name, st_astext(geom) from custom_region where name = '" + name + "'");
            if (resultSet.next()) {
                objectNode.set("multipolygon", multiPolygonStringToArrayNode(resultSet.getString("st_astext")));
            } else {
                objectNode.put("multipolygon", (Short)null);
            }
            return objectNode;
        } catch (InterruptedException | ExecutionException | TimeoutException | SQLException | IOException | NullPointerException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }

    @GetMapping("/analyze")
    public ObjectNode realtimeAnalyze(HttpServletRequest httpServletRequest,
                                             @RequestParam("file") MultipartFile uploadFile) {
        try {
            if (uploadFile.isEmpty() || uploadFile.getSize() == 0) {
                throw new IllegalParameterException();
            }
//            String path = httpServletRequest.getContextPath();
            String fileName = uploadFile.getOriginalFilename();
            File file = new File(Optional.ofNullable(fileName).orElse("new_file_" + System.currentTimeMillis()));
            uploadFile.transferTo(file);
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String header = reader.readLine();
            String[] headers = header.split(",");

            int addressIndex = 0;
            for (; addressIndex < headers.length; addressIndex += 1) {
                if (headers[addressIndex].equals("address")) {
                    break;
                }
            }

            ObjectNode objectNode = objectMapper.createObjectNode();
            objectNode.put("type", "FeatureCollection");
            ArrayNode arrayNode = objectMapper.createArrayNode();
            while (true) {
                String string = reader.readLine();
                if (string == null) {
                    break;
                }
                String[] strings = string.split(",");
                ObjectNode result = queryGeo(httpServletRequest, strings[addressIndex]);
                ObjectNode tempObjectNode = objectMapper.createObjectNode();
                tempObjectNode.put("type", "Feature");
                tempObjectNode.put("weight", 1);
                ObjectNode insideObjectNode = objectMapper.createObjectNode();
                insideObjectNode.put("type", "Point");
                ArrayNode insideArrayNode = objectMapper.createArrayNode();
                insideArrayNode.add(Double.parseDouble(result.get("lon").asText()));
                insideArrayNode.add(Double.parseDouble(result.get("lat").asText()));
                insideObjectNode.set("coordinates", insideArrayNode);
                tempObjectNode.set("geometry", insideObjectNode);
                arrayNode.add(tempObjectNode);
            }
            objectNode.set("features", arrayNode);
            log.printExecuteOkInfo(httpServletRequest);
            return objectNode;
        } catch (IOException | IllegalStateException | IllegalParameterException e) {
            log.printExceptionOccurredError(httpServletRequest, e);
            return objectMapper.createObjectNode().put("exception", e.getClass().getSimpleName());
        }
    }
}
