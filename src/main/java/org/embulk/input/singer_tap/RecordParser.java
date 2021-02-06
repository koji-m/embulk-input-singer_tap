package org.embulk.input.singer_tap;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.spi.type.Type;
import org.msgpack.value.Value;


public interface RecordParser {
    Value parse(JsonNode node);
    Type embulkType();
}
