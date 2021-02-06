package org.embulk.input.singer_tap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.ArrayList;
import java.util.List;

public class ArrayParser implements  RecordParser {
    private RecordParser elementParser;

    public ArrayParser(JsonNode node) throws Exception {
        elementParser = ParserGenerator.generateParser(node);
    }

    public Value parse(JsonNode node) {
        ArrayNode arrNode = (ArrayNode) node;
        List<Value> list = new ArrayList<>();
        for (JsonNode element : arrNode) {
            list.add(elementParser.parse(element));
        }

        return ValueFactory.newArray(list);
    }

    public Type embulkType() { return Types.JSON; }
}
