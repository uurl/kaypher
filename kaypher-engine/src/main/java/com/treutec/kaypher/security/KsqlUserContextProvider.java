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
package com.treutec.kaypher.security;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import com.treutec.kaypher.services.ConfiguredKafkaClientSupplier;

import java.security.Principal;
import java.util.function.Supplier;

/**
 * Provides access to clients required to communicate with remote services using the context of
 * a specified user name and/or credentials.
 * <p/>
 * This context is used by KAYPHER to access Kafka and/or Schema Registry resources when a user
 * executes a KAYPHER command so it can access those resources using the same user's permissions.
 * <p/>
 * Note: This context is used only for non-persistent commands.
 */
public interface KaypherUserContextProvider {
  /**
   * Constructs a {@link ConfiguredKafkaClientSupplier} to access Kafka resources on the
   * context of the specified user {@code principal}.
   * </p>
   * {@code Note:} The {@code ConfiguredKafkaClientSupplier} is required to get a
   * {@code KafkaClientSupplier} with injected configurations that authenticates the user
   * when accessing Kafka.
   *
   * @param principal The {@link Principal} whose credentials will be used.
   * @return {@link ConfiguredKafkaClientSupplier}.
   */
  ConfiguredKafkaClientSupplier getKafkaClientSupplier(Principal principal);

  /**
   * Constructs a {@link io.confluent.kafka.schemaregistry.client.SchemaRegistryClient} supplier
   * with to access Schema Registry resources on the context of the specified user
   * {@code principal}.
   *
   * @param principal The {@link Principal} whose credentials will be used.
   * @return {@link io.confluent.kafka.schemaregistry.client.SchemaRegistryClient} supplier.
   */
  Supplier<SchemaRegistryClient> getSchemaRegistryClientFactory(Principal principal);
}
