package org.embulk.input.singer_tap;

import com.fasterxml.jackson.databind.JsonNode;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

public class NullableArrayParser extends ArrayParser {
    public NullableArrayParser(JsonNode node) throws Exception {
        super(node);
    }

    @Override
    public Value parse(JsonNode node) {
        if (node.isNull()) {
            return ValueFactory.newNil();
        }
        return super.parse(node);
    }
}
