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

package com.treutec.kaypher.serde;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.util.KaypherConfig;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;

interface SerdeFactories {

  /**
   * Create {@link Serde} for supported KAYPHER formats.
   *
   * @param format required format.
   * @param schema persitence schema
   * @param ksqlConfig system config.
   * @param schemaRegistryClientFactory the sr client factory.
   * @param type the value type.
   * @param <T> the value type.
   */
  <T> Serde<T> create(
      FormatInfo format,
      PersistenceSchema schema,
      KaypherConfig ksqlConfig,
      Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      Class<T> type
  );
}
