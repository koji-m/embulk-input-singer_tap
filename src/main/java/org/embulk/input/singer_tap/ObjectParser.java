package org.embulk.input.singer_tap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class ObjectParser implements RecordParser {
    private Map<String, RecordParser> fieldParser;

    public ObjectParser(ObjectNode node) throws Exception {
        fieldParser = new LinkedHashMap<>();
        for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> e = it.next();
            fieldParser.put(e.getKey(), ParserGenerator.generateParser(e.getValue()));
        }
    }

    @Override
    public Value parse(JsonNode node) {
        ObjectNode objNode = (ObjectNode) node;
        Map<Value, Value> map = new HashMap<>();

        for (Map.Entry<String, RecordParser> entry : fieldParser.entrySet()) {
            String key = entry.getKey();
            JsonNode valNode = objNode.get(key);
            Value val;
            if (valNode == null) {
                val = ValueFactory.newNil();
            }
            else {
                val = fieldParser.get(key).parse(valNode);
            }
            map.put(ValueFactory.newString(key), val);
        }

        return ValueFactory.newMap(map);
    }

    public Map<String, RecordParser> properties() {
        return fieldParser;
    }

    public Type embulkType() { return Types.JSON; }
}
