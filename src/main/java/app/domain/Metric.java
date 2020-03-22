package app.domain;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Optional;

public class Metric {

    private String type;
    private String field;
    private String alias;
    private boolean isInData;

    public Metric(JsonNode metricJson) {
        type = Optional.ofNullable(metricJson).map((x) -> x.get("type")).map(JsonNode::asText).orElse(null);
        field = Optional.ofNullable(metricJson).map((x) -> x.get("field")).map(JsonNode::asText).orElse(null);
        alias = Optional.ofNullable(metricJson).map((x) -> x.get("alias")).map(JsonNode::asText).orElse(null);
        isInData = Optional.ofNullable(metricJson).map((x) -> x.get("in_data")).map(JsonNode::asBoolean).orElse(false);
    }

    public String getField() {
        return field;
    }

    public String getAlias() {
        return alias;
    }

    public String toSql() {
        String sqlAfterDataAdjust = isInData && field != null ? "cast(data->>'" + field + "' as float4)" : Optional.ofNullable(field).orElse("0");
        String sqlAfterTypeAdjust = type == null ? sqlAfterDataAdjust : type + "(" + sqlAfterDataAdjust + ")";
        return alias == null ? sqlAfterTypeAdjust + " as " + field : sqlAfterTypeAdjust + " as " + alias;
    }
}
