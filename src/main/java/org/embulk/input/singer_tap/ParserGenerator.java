package org.embulk.input.singer_tap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ParserGenerator {
    public static RecordParser generateParser(JsonNode node) throws Exception {
        String type;
        Boolean nullable = false;
        JsonNode type_node = node.get("type");
        if (type_node.isTextual()) {
            type = type_node.asText();
        }
        else if (type_node.isArray()) {
            ArrayNode types = (ArrayNode) type_node;
            int len = types.size();
            if (len == 2) {
                String type_0 = types.get(0).asText();
                String type_1 = types.get(1).asText();
                if (type_0.equals("null")) {
                    nullable = true;
                    type = type_1;
                }
                else if (type_1.equals("null")) {
                    nullable = true;
                    type = type_0;
                }
                else {
                    type = "undefined";
                }
            }
            else if (len == 1){
                type = types.get(0).asText();
            }
            else {
                throw new Exception("number of possible types must be 1 or 2!");
            }
        }
        else {
            throw new Exception("invalid type!");
        }

        if (nullable) {
            switch (type) {
                case "string":
                    return new NullableStringParser();
                case "number":
                    return new NumberParser();
                case "integer":
                    return new NullableIntegerParser();
                case "boolean":
                    return new NullableBooleanParser();
                case "object":
                    return new NullableObjectParser((ObjectNode) (node.get("properties")));
                case "array":
                    return new NullableArrayParser(node.get("items"));
                default:
                    throw new Exception("Undefined type");
            }
        }
        switch (type) {
            case "string":
                return new StringParser();
            case "number":
                return new NumberParser();
            case "integer":
                return new IntegerParser();
            case "boolean":
                return new BooleanParser();
            case "object":
                return new ObjectParser((ObjectNode) (node.get("properties")));
            case "array":
                return new ArrayParser(node.get("items"));
            default:
                throw new Exception("Undefined type");
        }
    }
}
