/*
 * Copyright © 2019 Christopher Matta (chris.matta@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.birdie.kafka.connect.smt;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.jose4j.jwa.AlgorithmConstraints;
import org.jose4j.jwe.ContentEncryptionAlgorithmIdentifiers;
import org.jose4j.jwe.JsonWebEncryption;
import org.jose4j.jwe.KeyManagementAlgorithmIdentifiers;
import org.jose4j.jwk.JsonWebKey;
import org.jose4j.lang.JoseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public abstract class Jose<R extends ConnectRecord<R>> implements Transformation<R> {
  private interface ConfigName {
    String KEYS = "keys";
    String SKIP_ON_ERROR = "skip-on-error";
  }
  public static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(ConfigName.KEYS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
      "Base64-encoded JWT encryption keys (comma separated).")
    .define(ConfigName.SKIP_ON_ERROR, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, "If true, ignores decryption errors and skip event.");

  private static final Logger LOGGER = LoggerFactory.getLogger(Jose.class);

  private List<JsonWebKey> jwks = new ArrayList<>();
  private ObjectMapper mapper = new ObjectMapper();
  private boolean skipOnError;

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    skipOnError = config.getBoolean(ConfigName.SKIP_ON_ERROR);

    try {
      for (String base64EncodedKey: config.getString(ConfigName.KEYS).split(",")) {
        String keyAsString = new String(
                Base64.getDecoder().decode(base64EncodedKey)
        );

        jwks.add(JsonWebKey.Factory.newJwk(keyAsString));
      }

    } catch (JoseException e) {
      throw new IllegalArgumentException("The provided encryption key is not valid. It should be a base64-encoded JWK key.", e);
    }

    if (jwks.size() == 0) {
      throw new IllegalStateException("No encryption keys have been configured.");
    }
  }

  @Override
  public R apply(R record) {
    if (operatingSchema(record) == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  private R applySchemaless(R record) {
    throw new IllegalArgumentException("The JOSE JMT only supports messages with byte schema.");
  }

  private R applyWithSchema(R record) {
    Object value = operatingValue(record);
    Schema schema = operatingSchema(record);

    // With the `value.converter: org.apache.kafka.connect.converters.ByteArrayConverter` configuration, we will receive a "byte schema"
    // https://github.com/a0x8o/kafka/blob/master/connect/runtime/src/main/java/org/apache/kafka/connect/converters/ByteArrayConverter.java#L65
    if (!schema.type().equals(Schema.Type.BYTES)) {
      throw new IllegalArgumentException("The JOSE SMT only supports bytes schemas, got "+schema.toString()+".");
    }

    if (value != null && !(value instanceof byte[])) {
      throw new DataException("Jose is not compatible with objects of type " + value.getClass());
    }

    String serialized = new String((byte[]) value);

    for (JsonWebKey jwk: jwks) {
      // Create a new Json Web Encryption object
      JsonWebEncryption jwe = new JsonWebEncryption();
      jwe.setKey(jwk.getKey());

      AlgorithmConstraints algConstraints = new AlgorithmConstraints(AlgorithmConstraints.ConstraintType.PERMIT, KeyManagementAlgorithmIdentifiers.DIRECT);
      jwe.setAlgorithmConstraints(algConstraints);
      AlgorithmConstraints encConstraints = new AlgorithmConstraints(AlgorithmConstraints.ConstraintType.PERMIT, ContentEncryptionAlgorithmIdentifiers.AES_256_GCM);
      jwe.setContentEncryptionAlgorithmConstraints(encConstraints);

      try {
        // Transform the flattened JWE in its compacted format
        Map<String, String> flattenedJweMap = mapper.readValue(serialized, Map.class);
        String compactedJwe = String.join(".",
                flattenedJweMap.get("protected"),
                "", // Recipients, not used.
                flattenedJweMap.get("iv"), flattenedJweMap.get("ciphertext"), flattenedJweMap.get("tag"));

        jwe.setCompactSerialization(compactedJwe);
      } catch (JacksonException e) {
        if (skipOnError) {
          LOGGER.error("Flattened JWE could not be decoded.", e);

          break;
        }

        throw new IllegalArgumentException("Flattened JWE could not be decoded ("+LoggingContext.createContext(record)+").", e);
      } catch (JoseException e) {
        throw new IllegalArgumentException("Could not create compacted JWE ("+LoggingContext.createContext(record)+")", e);
      }

      try {
        // Decrypt the JWE
        String plaintext = jwe.getPlaintextString();

        return newRecord(record, schema, plaintext.getBytes());
      } catch (JoseException e) {
        LOGGER.info("Could not decrypt event with the provided key, trying next one.", e);
      }
    }

    if (skipOnError) {
      LOGGER.error("Could not decrypt message ({}) with any of the provided keys. Skipping as configured.", LoggingContext.createContext(record));
    } else {
      throw new IllegalArgumentException("Message could not be decrypted with any encryption key ("+LoggingContext.createContext(record)+")");
    }

    return newRecord(record, schema, value);
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    // Nothing to do...
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class DecryptValue<R extends ConnectRecord<R>> extends Jose<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }
  }
}
