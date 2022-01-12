package com.github.hackerxu.kafka.connect;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class JsonToSchemaConverterConfig extends AbstractKafkaSchemaSerDeConfig {

    public static final String SCHEMA_ID_CONFIG = "schema.id";
    public static final int SCHEMA_ID_DEFAULT = -1;
    public static final String SCHEMA_ID_DOC =
            "To define the id in registry schema, if use default -1, it will raise Exception";
    public static final String HOIST_FIELD_CONFIG = "hoist.field";
    public static final String HOIST_FIELD_DEFAULT = null;
    public static final String HOIST_FIELD_DOC =
            "Hoist field like transform";
    public static final String HOIST_FIELD_TIMESTAMP_CONFIG = "hoist.field.timestamp";
    public static final String HOIST_FIELD_TIMESTAMP_DEFAULT = null;
    public static final String HOIST_FIELD_TIMESTAMP_DOC =
            "Hoist field like transform";

    public static ConfigDef getAddConfigDef() {
        ConfigDef config = baseConfigDef();
        config
                .define(SCHEMA_ID_CONFIG, ConfigDef.Type.INT, SCHEMA_ID_DEFAULT,
                        ConfigDef.Importance.MEDIUM, SCHEMA_ID_DOC)
                .define(HOIST_FIELD_CONFIG, ConfigDef.Type.STRING, HOIST_FIELD_DEFAULT,
                        ConfigDef.Importance.MEDIUM, HOIST_FIELD_DOC)
                .define(HOIST_FIELD_TIMESTAMP_CONFIG, ConfigDef.Type.STRING, HOIST_FIELD_TIMESTAMP_DEFAULT,
                ConfigDef.Importance.MEDIUM, HOIST_FIELD_TIMESTAMP_DOC);


        return config;
    }

    public JsonToSchemaConverterConfig(Map<?, ?> props) {
        super(getAddConfigDef(), props);
    }

    public int getSchemaId() {
        return this.getInt(SCHEMA_ID_CONFIG);
    }

    public String getHoistField() {
        return this.getString(HOIST_FIELD_CONFIG);
    }

    public String getHoistFieldTimestamp() {
        return this.getString(HOIST_FIELD_TIMESTAMP_CONFIG);
    }

}
