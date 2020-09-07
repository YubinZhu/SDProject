package app.domain;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Arrays;
import java.util.Optional;

public class Dimension {

    private String field;

    public Dimension(JsonNode dimensionJson) {
        field = Optional.ofNullable(dimensionJson).map(JsonNode::asText).orElse("label");
    }

    public String getField() {
        return field;
    }

    public String toSql() {
        if (Arrays.asList("id", "label", "create_time", "entity_name", "province", "city", "lon", "lat", "category", "geom").contains(field)) {
            return field;
        }
        return "data->>'" + field + "' as " + field;
    }

    public String toSqlInGroupByClause() {
        if (Arrays.asList("id", "label", "create_time", "entity_name", "province", "city", "lon", "lat", "category", "geom").contains(field)) {
            return field;
        }
        return "data->>'" + field + "'";
    }
}
