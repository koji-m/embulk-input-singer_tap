package org.embulk.input.singer_tap;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

public class BooleanParser implements RecordParser {
    @Override
    public Value parse(JsonNode node) {
        return ValueFactory.newBoolean(node.asBoolean());
    }

    public Type embulkType() { return Types.BOOLEAN; }
}
