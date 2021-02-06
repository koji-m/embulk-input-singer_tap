package org.embulk.input.singer_tap;

import com.fasterxml.jackson.databind.JsonNode;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

public class NullableStringParser extends StringParser {
    @Override
    public Value parse(JsonNode node) {
        if (node.isNull()) {
            return ValueFactory.newNil();
        }
        return ValueFactory.newString(node.asText());
    }
}
