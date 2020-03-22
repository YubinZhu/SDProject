package app.domain;

import com.fasterxml.jackson.databind.JsonNode;

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
        return field;
    }
}
