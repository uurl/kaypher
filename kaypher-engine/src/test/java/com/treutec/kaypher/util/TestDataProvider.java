/*
 * Copyright 2019 Treu Techologies
 *
 * See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.treutec.kaypher.util;

import com.treutec.kaypher.GenericRow;
import com.treutec.kaypher.schema.kaypher.PhysicalSchema;
import java.util.Map;
import java.util.Objects;

public abstract class TestDataProvider {
  private final String topicName;
  private final String kaypherSchemaString;
  private final String key;
  private final PhysicalSchema schema;
  private final Map<String, GenericRow> data;
  private final String kstreamName;

  TestDataProvider(
      final String namePrefix,
      final String kaypherSchemaString,
      final String key,
      final PhysicalSchema schema,
      final Map<String, GenericRow> data
  ) {
    this.topicName = Objects.requireNonNull(namePrefix, "namePrefix") + "_TOPIC";
    this.kstreamName =  namePrefix + "_KSTREAM";
    this.kaypherSchemaString = Objects.requireNonNull(kaypherSchemaString, "kaypherSchemaString");
    this.key = Objects.requireNonNull(key, "key");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.data = Objects.requireNonNull(data, "data");
  }

  public String topicName() {
    return topicName;
  }

  public String kaypherSchemaString() {
    return kaypherSchemaString;
  }

  public String key() {
    return key;
  }

  public PhysicalSchema schema() {
    return schema;
  }

  public Map<String, GenericRow> data() {
    return data;
  }

  public String kstreamName() {
    return kstreamName;
  }
}
