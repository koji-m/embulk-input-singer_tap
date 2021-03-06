package org.embulk.input.singer_tap;

import java.io.*;
import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.embulk.config.*;
import org.embulk.spi.*;
import org.msgpack.value.Value;


public class SingerTapInputPlugin
        implements InputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("tap_command")
        public String getTapCommand();

        @Config("config")
        public String getConfig();

        @Config("catalog")
        @ConfigDefault("null")
        public Optional<String> getCatalog();

        @Config("properties")
        @ConfigDefault("null")
        public Optional<String> getProperties();

        @Config("input_state")
        @ConfigDefault("null")
        public Optional<String> getInputState();

        @Config("output_state")
        @ConfigDefault("null")
        public Optional<String> getOutputState();

        String getSchemaFileName();
        void setSchemaFileName(String schemaFile);

        List<String> getCommandLine();
        void setCommandLine(List<String> commandLine);

        @ConfigInject
        BufferAllocator getBufferAllocator();

    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        String command = task.getTapCommand() + " --config " + task.getConfig();

        File schemaFile;
        if (task.getCatalog().isPresent()) {
            if (task.getProperties().isPresent()) {
                throw new ConfigException("only one of 'catalog' or 'properties' parameter is needed");
            }
            String schemaFileName = task.getCatalog().get();
            schemaFile = new File(schemaFileName);
            task.setSchemaFileName(schemaFileName);
            command = command + " --catalog " + schemaFileName;
        }
        else {
            if (!task.getProperties().isPresent()) {
                throw new ConfigException("'catalog' or 'properties' parameter is needed");
            }
            String schemaFileName = task.getProperties().get();
            schemaFile = new File(schemaFileName);
            task.setSchemaFileName(schemaFileName);
            command = command + " --properties " + schemaFileName;
        }
        Schema schema = generateSchema(schemaFile);

        if (task.getInputState().isPresent()) {
            String stateFileName = task.getInputState().get();
            command = command + " --state " + stateFileName;
        }

        List<String> cmdline = new ArrayList<>(Arrays.asList("sh", "-c"));
        cmdline.add(command);
        task.setCommandLine(cmdline);

        int taskCount = 1;  // number of run() method calls

        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TaskReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        BufferAllocator allocator = task.getBufferAllocator();

        RecordParser parser;
        try {
            JsonNode schemaNode = getSchema(new File(task.getSchemaFileName()));
            parser = ParserGenerator.generateParser(schemaNode);
        }
        catch (Exception e) {
            throw new ConfigException(e.getMessage());
        }

        List<String> cmdline = task.getCommandLine();
        ProcessBuilder pb = new ProcessBuilder(cmdline);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        String state = "";
        try {
            Process process = pb.start();
            InputStream stream = process.getInputStream();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(stream))) {
                String line;
                JsonNode root;
                PageBuilder pageBuilder = new PageBuilder(allocator, schema, output);;
                ObjectMapper recordMapper = new ObjectMapper();
                while ((line = br.readLine()) != null) {
                    root = recordMapper.readTree(line);
                    String type = root.get("type").asText();
                    if (type.equals("RECORD")) {
                        JsonNode recordNode = root.get("record");
                        Value record = parser.parse(recordNode);
                        if (!(record.isMapValue())) {
                            throw new DataException("invalid record");
                        }
                        Map<String, Value> rec = new HashMap<>();
                        for (Map.Entry<Value, Value> entry : record.asMapValue().entrySet()) {
                            rec.put(entry.getKey().asStringValue().asString(), entry.getValue());
                        }
                        for (Column column : pageBuilder.getSchema().getColumns()) {
                            setColumn(column, rec, pageBuilder);
                        }
                        pageBuilder.addRecord();
                    }
                    else if (type.equals("SCHEMA")) {

                    }
                    else if (type.equals("STATE")) {
                        state = root.get("value").toString();
                    }
                    else {
                        throw new DataException("invalid message type: " + type);
                    }
                }
                pageBuilder.finish();
            }
        }
        catch (IOException e) {
            throw new DataException(e.getMessage());
        }
        finally {
            if (task.getOutputState().isPresent()) {
                String statePath = task.getOutputState().get();
                try (FileOutputStream writer = new FileOutputStream(statePath)) {
                    writer.write(state.getBytes());
                }
                catch (Exception e) {
                    throw new DataException(e.getMessage());
                }
            }
        }

        return Exec.newTaskReport();
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }

    private JsonNode getSchema(File catalog) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode streams = mapper.readTree(catalog).get("streams");
            JsonNode schemaNode = null;
            for (JsonNode stream : streams) {
                JsonNode schema = stream.get("schema");
                JsonNode selected = schema.get("selected");
                if (selected != null && selected.asBoolean()) {
                    schemaNode = schema;
                    break;
                }
            }
            if (schemaNode == null) {
                throw new ConfigException("schema not selected");
            }
            return schemaNode;
        }
        catch (Exception e) {
            throw new ConfigException(e.getMessage());
        }
    }

    private Schema generateSchema(File catalog) throws ConfigException {
        Schema.Builder builder = Schema.builder();
        try {
            JsonNode schemaNode = getSchema(catalog);
            RecordParser parser = ParserGenerator.generateParser(schemaNode);
            if (!(parser instanceof ObjectParser)) {
                throw new DataException("invalid schema");
            }
            ObjectParser toplevelParser = (ObjectParser) parser;
            for (Map.Entry<String, RecordParser> entry : toplevelParser.properties().entrySet()) {
                String colName = entry.getKey();
                RecordParser colParser = entry.getValue();
                builder.add(colName, colParser.embulkType());
            }
        }
        catch (Exception e) {
            throw new ConfigException(e.getMessage());
        }
        return builder.build();
    }

    private void setColumn(Column column, Map<String, Value> mapValue, PageBuilder pageBuilder) {
        String key = column.getName();
        Value val = mapValue.get(key);
        if (val.isStringValue()) {
            pageBuilder.setString(column, val.asStringValue().asString());
        }
        else if (val.isIntegerValue()) {
            pageBuilder.setLong(column, val.asIntegerValue().asLong());
        }
        else if (val.isFloatValue()) {
            pageBuilder.setDouble(column, val.asFloatValue().toDouble());
        }
        else if (val.isBooleanValue()) {
            pageBuilder.setBoolean(column, val.asBooleanValue().getBoolean());
        }
        else if (val.isMapValue() || val.isArrayValue()) {
            pageBuilder.setJson(column, val);
        }
        else if (val.isNilValue()) {
            pageBuilder.setNull(column);
        }
        else {
            throw new DataException("invalid type of record for column: " + key + ": "+ val.getValueType());
        }
    }
}
