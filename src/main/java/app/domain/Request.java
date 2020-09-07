package app.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Request {

    private List<Dimension> dimensions;
    private List<Metric> metrics;
    private List<Filter> filters;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String SELECT = "select";
    private static final String FROM = "from";
    private static final String FROM_TABLE = "sd_entity_data";
    private static final String WHERE = "where";
    private static final String GROUP_BY = "group by";

    private static final String COMMA_SPACE = ", ";
    private static final String SPACE = " ";
    private static final String SPACE_AND_SPACE = " and ";

    public Request(JsonNode requestJson) {
        JsonNode dimensionsNode = Optional.ofNullable(requestJson).map((x) -> (x.get("dimensions"))).orElse(objectMapper.createArrayNode());
        JsonNode metricsNode = Optional.ofNullable(requestJson).map((x) -> (x.get("metrics"))).orElse(objectMapper.createArrayNode());
        JsonNode filtersNode = Optional.ofNullable(requestJson).map((x) -> (x.get("filters"))).orElse(objectMapper.createArrayNode());

        dimensions = new ArrayList<>();
        metrics = new ArrayList<>();
        filters = new ArrayList<>();

        if (dimensionsNode.size() > 0) {
            for (JsonNode dimensionNode : dimensionsNode) {
                dimensions.add(new Dimension(dimensionNode));
            }
        }

        if (metricsNode.size() > 0) {
            for (JsonNode metricNode : metricsNode) {
                metrics.add(new Metric(metricNode));
            }
        }

        if (filtersNode.size() > 0) {
            for (JsonNode filterNode : filtersNode) {
                filters.add(new Filter(filterNode));
            }
        }
    }

    public List<Dimension> getDimensions() {
        return dimensions;
    }

    public List<Metric> getMetrics() {
        return metrics;
    }

    public String toSql() {
        if (metrics.size() == 0 && dimensions.size() == 0) {
            return "";
        }

        List<String> dimensionSqlList = new ArrayList<>();
        for (Dimension dimension : dimensions) {
            dimensionSqlList.add(dimension.toSql());
        }
        String dimensionSql = String.join(COMMA_SPACE, dimensionSqlList);
        List<String> dimensionSqlListInGroupByClause = new ArrayList<>();
        for (Dimension dimension : dimensions) {
            dimensionSqlListInGroupByClause.add(dimension.toSqlInGroupByClause());
        }
        String dimensionSqlInGroupByClause = String.join(COMMA_SPACE, dimensionSqlListInGroupByClause);
        List<String> metricSqlList = new ArrayList<>();
        for (Metric metric : metrics) {
            metricSqlList.add(metric.toSql());
        }
        String metricSql = String.join(COMMA_SPACE, metricSqlList);
        List<String> filterSqlList = new ArrayList<>();
        for (Filter filter : filters) {
            filterSqlList.add(filter.toSql());
        }
        String filterSql = String.join(SPACE_AND_SPACE, filterSqlList);

        List<String> sqlList = new ArrayList<>();
        sqlList.add(SELECT);
        if (dimensionSqlList.size() == 0) {
            sqlList.add(metricSql);
        } else if (metricSqlList.size() == 0) {
            sqlList.add(dimensionSql);
        } else {
            sqlList.add(dimensionSql + COMMA_SPACE + metricSql);
        }
        sqlList.add(FROM);
        sqlList.add(FROM_TABLE);
        if (filterSqlList.size() != 0) {
            sqlList.add(WHERE);
            sqlList.add(filterSql);
        }
        if (dimensionSqlList.size() != 0) {
            sqlList.add(GROUP_BY);
            sqlList.add(dimensionSqlInGroupByClause);
        }

        return String.join(SPACE, sqlList);
    }

    public void fix() {
        if (metrics.size() == 0 && dimensions.size() == 0) {
            return;
        }
        filters.add(new Filter(objectMapper.createObjectNode()
                .put("field", "is_valid")
                .put("type", "equal")
                .set("conditions", objectMapper.createArrayNode().add("true")))
        );
        if (!containsFilter("label")) {
            filters.add(new Filter(objectMapper.createObjectNode()
                    .put("field", "label")
                    .put("type", "in")
                    .set("conditions", objectMapper.createArrayNode().add("test"))));
        }
    }

    public boolean containsFilter(String field) {
        if (field == null) {
            return false;
        }
        for (Filter filter : filters) {
            if (field.equals(filter.getField())) {
                return true;
            }
        }
        return false;
    }
}
