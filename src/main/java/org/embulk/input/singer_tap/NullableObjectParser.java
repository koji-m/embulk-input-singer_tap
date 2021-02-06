package org.embulk.input.singer_tap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

public class NullableObjectParser extends ObjectParser {
    public NullableObjectParser(ObjectNode node) throws Exception {
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
