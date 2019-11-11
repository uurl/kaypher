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
package com.treutec.kaypher.embedded;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import com.treutec.kaypher.ServiceInfo;
import com.treutec.kaypher.engine.KaypherEngine;
import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.logging.processing.ProcessingLogContext;
import com.treutec.kaypher.query.id.SequentialQueryIdGenerator;
import com.treutec.kaypher.services.DefaultConnectClient;
import com.treutec.kaypher.services.KafkaTopicClient;
import com.treutec.kaypher.services.KafkaTopicClientImpl;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.services.TestServiceContext;
import com.treutec.kaypher.statement.Injectors;
import com.treutec.kaypher.util.KaypherConfig;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

public final class KaypherContextTestUtil {

  private static final AtomicInteger COUNTER = new AtomicInteger();

  private KaypherContextTestUtil() {
  }

  public static KaypherContext create(
      final KaypherConfig kaypherConfig,
      final SchemaRegistryClient schemaRegistryClient,
      final FunctionRegistry functionRegistry
  ) {
    final KafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();

    final Admin adminClient = clientSupplier
        .getAdmin(kaypherConfig.getKaypherAdminClientConfigProps());

    final KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);

    final ServiceContext serviceContext = TestServiceContext.create(
        clientSupplier,
        adminClient,
        kafkaTopicClient,
        () -> schemaRegistryClient,
        new DefaultConnectClient(
            kaypherConfig.getString(KaypherConfig.CONNECT_URL_PROPERTY),
            Optional.empty())
    );

    final String metricsPrefix = "instance-" + COUNTER.getAndIncrement() + "-";

    final KaypherEngine engine = new KaypherEngine(
        serviceContext,
        ProcessingLogContext.create(),
        functionRegistry,
        ServiceInfo.create(kaypherConfig, metricsPrefix),
        new SequentialQueryIdGenerator()
    );

    return new KaypherContext(
        serviceContext,
        kaypherConfig,
        engine,
        Injectors.DEFAULT
    );
  }
}
