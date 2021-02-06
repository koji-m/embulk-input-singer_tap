package org.embulk.input.singer_tap;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

public class NumberParser implements RecordParser {
    @Override
    public Value parse(JsonNode node) {
        return ValueFactory.newFloat(node.asDouble());
    }

    public Type embulkType() { return Types.DOUBLE; }
}
