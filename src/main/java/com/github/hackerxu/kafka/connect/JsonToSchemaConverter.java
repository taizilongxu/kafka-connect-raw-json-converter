/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.github.hackerxu.kafka.connect;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.connect.json.JsonSchemaData;
import io.confluent.connect.json.JsonSchemaDataConfig;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.confluent.kafka.serializers.json.*;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.SchemaBuilderException;
import org.apache.kafka.connect.storage.Converter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.common.utils.Utils.mkSet;


/**
 * Implementation of Converter that supports raw JSON with hema.
 */
public class JsonToSchemaConverter extends AbstractKafkaSchemaSerDe implements Converter {
  private static final Logger log = LoggerFactory.getLogger(JsonToSchemaConverter.class);

  private SchemaRegistryClient schemaRegistry;
  private Serializer serializer;
  private Integer VERSION=1;

  private boolean isKey;
  private JsonSchemaData jsonSchemaData;

  private static final JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.withExactBigDecimals(true);
  private final ObjectMapper objectMapper = new ObjectMapper();
  private int registrySchemaId;
  private String hoistField;
  private String hoistFieldTimestamp;

  public JsonToSchemaConverter() {

  }

  @VisibleForTesting
  public JsonToSchemaConverter(SchemaRegistryClient client) {
    schemaRegistry = client;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    JsonToSchemaConverterConfig jsonToSchemaConverterConfig = new JsonToSchemaConverterConfig(configs);

    if (schemaRegistry == null) {
      schemaRegistry = new CachedSchemaRegistryClient(
          jsonToSchemaConverterConfig.getSchemaRegistryUrls(),
          jsonToSchemaConverterConfig.getMaxSchemasPerSubject(),
          Collections.singletonList(new JsonSchemaProvider()),
          configs,
          jsonToSchemaConverterConfig.requestHeaders()
      );
    }

    serializer = new Serializer(configs, schemaRegistry);
    jsonSchemaData = new JsonSchemaData(new JsonSchemaDataConfig(configs));

    mkSet(
            // this ensures that the JsonDeserializer maintains full precision on
            // floating point numbers that cannot fit into float64
            DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS
    ).forEach(objectMapper::enable);
    objectMapper.setNodeFactory(JSON_NODE_FACTORY);
    // config
    registrySchemaId = jsonToSchemaConverterConfig.getSchemaId();
    hoistField = jsonToSchemaConverterConfig.getHoistField();
    hoistFieldTimestamp = jsonToSchemaConverterConfig.getHoistFieldTimestamp();
  }

  @TestOnly
  public void configHoist(Map<String, String> configs) {
    JsonToSchemaConverterConfig jsonToSchemaConverterConfig = new JsonToSchemaConverterConfig(configs);
    hoistField = jsonToSchemaConverterConfig.getHoistField();
    hoistFieldTimestamp = jsonToSchemaConverterConfig.getHoistFieldTimestamp();
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    if (schema == null && value == null) {
      return null;
    }
    JsonSchema jsonSchema = jsonSchemaData.fromConnectSchema(schema);
    JsonNode jsonValue = jsonSchemaData.fromConnectData(schema, value);
    try {
      return serializer.serialize(topic, isKey, jsonValue, jsonSchema);
    } catch (SerializationException e) {
      throw new DataException(String.format("Converting Kafka Connect data to byte[] failed due to "
          + "serialization error of topic %s: ",
          topic),
          e
      );
    } catch (InvalidConfigurationException e) {
      throw new ConfigException(
          String.format("Failed to access data from topic %s :", topic),
          e
      );
    }
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    // This handles a tombstone message
    if (value == null) {
      return SchemaAndValue.NULL;
    }

    if (hoistField != null) {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct()
              .field(hoistField, Schema.STRING_SCHEMA);
      ObjectNode rootNode = objectMapper.createObjectNode();
      rootNode.put(hoistField, new String(value, StandardCharsets.UTF_8));
      // add hoist timestamp
      if (hoistFieldTimestamp != null) {
        schemaBuilder.field(hoistFieldTimestamp, Schema.STRING_SCHEMA);
        try {
          rootNode.put(hoistFieldTimestamp, objectMapper.readTree(value).get(hoistFieldTimestamp));
        } catch (IOException e) {
          log.error("Extract hoist field timestamp error: ", e);
        }
      }
      Schema schema = schemaBuilder.build();

      return new SchemaAndValue(schema, jsonSchemaData.toConnectData(schema, rootNode));
    }

    try {
      JsonNode jsonValue = objectMapper.readTree(value);
      JsonSchema jsonSchema = ((JsonSchema) schemaRegistry.getSchemaBySubjectAndId(topic, registrySchemaId));
      jsonSchema = jsonSchema.copy(VERSION);
      Schema schema = jsonSchemaData.toConnectSchema(jsonSchema);
      log.debug("Schema: " + jsonSchema.toString());
      log.debug("Schema fields: " + schema.fields());
      log.debug("Json value: " + jsonValue);

      return new SchemaAndValue(schema, jsonSchemaData.toConnectData(schema, jsonValue));
    } catch (SerializationException e) {
      throw new DataException(String.format("Converting byte[] to Kafka Connect data failed due to "
                      + "serialization error of topic %s: ",
              topic),
              e
      );
    } catch (InvalidConfigurationException e) {
      throw new ConfigException(
              String.format("Failed to access data from topic %s :", topic),
              e
      );
    } catch (RestClientException e) {
      throw new SchemaBuilderException(e);
    } catch (IOException e) {
      throw new SchemaBuilderException(e);
    }
  }

  private static class Serializer extends AbstractKafkaJsonSchemaSerializer {

    public Serializer(SchemaRegistryClient client, boolean autoRegisterSchema) {
      schemaRegistry = client;
      this.autoRegisterSchema = autoRegisterSchema;
    }

    public Serializer(Map<String, ?> configs, SchemaRegistryClient client) {

      this(client, false);
      configure(new KafkaJsonSchemaSerializerConfig(configs));
    }

    public byte[] serialize(String topic, boolean isKey, Object value, JsonSchema schema) {
      if (value == null) {
        return null;
      }
      return serializeImpl(getSubjectName(topic, isKey, value, schema), value, schema);
    }
  }
}
