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
package com.treutec.kaypher.services;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import com.treutec.kaypher.schema.registry.KaypherSchemaRegistryClientFactory;
import com.treutec.kaypher.util.KaypherConfig;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

public final class ServiceContextFactory {
  private ServiceContextFactory() {}

  public static ServiceContext create(
      final KaypherConfig kaypherConfig,
      final SimpleKaypherClient kaypherClient
  ) {
    return create(
        kaypherConfig,
        new DefaultKafkaClientSupplier(),
        new KaypherSchemaRegistryClientFactory(kaypherConfig, Collections.emptyMap())::get,
        new DefaultConnectClient(
            kaypherConfig.getString(KaypherConfig.CONNECT_URL_PROPERTY),
            Optional.empty()),
        kaypherClient
    );
  }

  public static ServiceContext create(
      final KaypherConfig kaypherConfig,
      final KafkaClientSupplier kafkaClientSupplier,
      final Supplier<SchemaRegistryClient> srClientFactory,
      final ConnectClient connectClient,
      final SimpleKaypherClient kaypherClient
  ) {
    final Admin adminClient = kafkaClientSupplier.getAdmin(
        kaypherConfig.getKaypherAdminClientConfigProps()
    );

    return new DefaultServiceContext(
        kafkaClientSupplier,
        adminClient,
        new KafkaTopicClientImpl(adminClient),
        srClientFactory,
        connectClient,
        kaypherClient
    );
  }
}
