package com.github.hackerxu.kafka.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static com.github.hackerxu.kafka.connect.JsonToSchemaConverterConfig.HOIST_FIELD_CONFIG;
import static com.github.hackerxu.kafka.connect.JsonToSchemaConverterConfig.HOIST_FIELD_TIMESTAMP_CONFIG;
import static org.apache.kafka.connect.json.JsonConverterConfig.SCHEMAS_ENABLE_CONFIG;
import static org.apache.kafka.connect.storage.ConverterConfig.TYPE_CONFIG;
import static org.junit.Assert.assertEquals;

class JsonToSchemaConverterTest {
    private final ResourceLoader loader = ResourceLoader.DEFAULT;

    private static final String TOPIC = "test";
    private static final Integer ID = 1;
    private static final Integer VERSION = 1;
    private static final String HOIST_FIELD = "json";
    private static final String HOIST_FIELD_TIMESTAMP = "timestamp";

    private static final ObjectMapper mapper = new ObjectMapper();

    private final SchemaRegistryClient schemaRegistry;
    private final JsonToSchemaConverter converter;
    private final JsonConverter jsonConverter;

    public Map<String, String> getConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", "http://fake-url");
        config.put(JsonToSchemaConverterConfig.SCHEMA_ID_CONFIG, String.valueOf(ID));
        config.put(SCHEMAS_ENABLE_CONFIG, "true");
        config.put(TYPE_CONFIG, "value");
        return config;
    }

    public Map<String, String> getHoistConfig() {
        Map<String, String> config = getConfig();
        config.put(HOIST_FIELD_CONFIG, HOIST_FIELD);
        config.put(HOIST_FIELD_TIMESTAMP_CONFIG, HOIST_FIELD_TIMESTAMP);
        return config;
    }

    public JsonToSchemaConverterTest() throws IOException, RestClientException {
        // schema registry mock
        schemaRegistry = new MockSchemaRegistryClient();
        Map<String, String> config = getConfig();

        converter = new JsonToSchemaConverter(schemaRegistry);
        converter.configure(config, false);
        jsonConverter = new JsonConverter();
        jsonConverter.configure(config);

        JsonNode rawSchemaJson = loader.readJsonNode("value.json");
        schemaRegistry.register(TOPIC, new JsonSchema(rawSchemaJson), VERSION, ID);
    }

    @Test
    void testToConnectData() throws RestClientException, IOException {
        InputStream is = JsonToSchemaConverterTest.class.getResourceAsStream("/json/raw.json");
        byte[] fileContent = ByteStreams.toByteArray(is);
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, fileContent);

        InputStream is2 = JsonToSchemaConverterTest.class.getResourceAsStream("/json/valueWithSchema.json");
        byte[] fileContent2 = ByteStreams.toByteArray(is2);

        SchemaAndValue schemaAndValue2 = jsonConverter.toConnectData(TOPIC, fileContent2);
        assertEquals(schemaAndValue.value().toString(), schemaAndValue2.value().toString());
        assertEquals(schemaAndValue.schema().toString(), schemaAndValue2.schema().toString());

    }

    @Test
    void testHoistField() throws IOException {
        converter.configHoist(getHoistConfig());

        InputStream is = JsonToSchemaConverterTest.class.getResourceAsStream("/json/raw.json");
        byte[] fileContent = ByteStreams.toByteArray(is);
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, fileContent);

        InputStream is2 = JsonToSchemaConverterTest.class.getResourceAsStream("/json/valueWithSchemaHoist.json");
        byte[] fileContent2 = ByteStreams.toByteArray(is2);

        SchemaAndValue schemaAndValue2 = jsonConverter.toConnectData(TOPIC, fileContent2);
        assertEquals(schemaAndValue.value().toString(), schemaAndValue2.value().toString());

    }
}