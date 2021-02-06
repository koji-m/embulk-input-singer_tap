package org.embulk.input.singer_tap;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

public class IntegerParser implements RecordParser {
    @Override
    public Value parse(JsonNode node) {
        return ValueFactory.newInteger(node.asLong());
    }

    public Type embulkType() { return Types.LONG; }
}
