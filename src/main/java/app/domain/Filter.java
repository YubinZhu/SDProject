package app.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Filter {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    enum Type {
        equal, in, or, between
    }

    private String field;
    private Type type;
    private List<String> conditions;

    public Filter(JsonNode filterJson) {
        field = Optional.ofNullable(filterJson).map((x) -> x.get("field")).map(JsonNode::asText).orElse("0");
        type = Type.valueOf(Optional.ofNullable(filterJson).map((x) -> x.get("type")).map(JsonNode::asText).orElse("in"));
        JsonNode conditionsJson = Optional.ofNullable(filterJson).map((x) -> x.get("conditions")).orElse(objectMapper.createArrayNode().add("0"));
        conditions = new ArrayList<>();
        for (JsonNode conditionJson : conditionsJson) {
            conditions.add(conditionJson.asText());
        }
    }

    public String getField() {
        return field;
    }

    public String toSql() {
        if (conditions.size() == 0) {
            return "0 = 0";
        }
        if (type.equals(Type.equal)) {
            return field + " = " + conditions.get(0);
        } else if (type.equals(Type.in)) {
            return field + " in ('" + String.join("', '", conditions) + "')";
        } else if (type.equals(Type.or)) {
            return "(" + field + " = " + String.join(" or " + field + " = ", conditions) + ")";
        } else if (type.equals(Type.between)) {
            if (conditions.size() < 2) {
                return field + " between 0 and 0";
            }
            return field + " between " + conditions.get(0) + " and " + conditions.get(1);
        } else {
            return "0 = 0";
        }
    }
}
