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

import static java.util.Objects.requireNonNull;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import com.treutec.kaypher.util.KaypherConfig;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.streams.KafkaClientSupplier;

/**
 * A real service context, initialized from a {@link KaypherConfig} instance.
 */
public class DefaultServiceContext implements ServiceContext {

  private final KafkaClientSupplier kafkaClientSupplier;
  private final Admin adminClient;
  private final KafkaTopicClient topicClient;
  private final Supplier<SchemaRegistryClient> srClientFactory;
  private final SchemaRegistryClient srClient;
  private final ConnectClient connectClient;
  private final SimpleKaypherClient kaypherClient;

  public DefaultServiceContext(
      final KafkaClientSupplier kafkaClientSupplier,
      final Admin adminClient,
      final KafkaTopicClient topicClient,
      final Supplier<SchemaRegistryClient> srClientFactory,
      final ConnectClient connectClient,
      final SimpleKaypherClient kaypherClient
  ) {
    this.kafkaClientSupplier = requireNonNull(kafkaClientSupplier, "kafkaClientSupplier");
    this.adminClient = requireNonNull(adminClient, "adminClient");
    this.topicClient = requireNonNull(topicClient, "topicClient");
    this.srClientFactory = requireNonNull(srClientFactory, "srClientFactory");
    this.srClient = requireNonNull(srClientFactory.get(), "srClient");
    this.connectClient = requireNonNull(connectClient, "connectClient");
    this.kaypherClient = requireNonNull(kaypherClient, "kaypherClient");
  }

  @Override
  public Admin getAdminClient() {
    return adminClient;
  }

  @Override
  public KafkaTopicClient getTopicClient() {
    return topicClient;
  }

  @Override
  public KafkaClientSupplier getKafkaClientSupplier() {
    return kafkaClientSupplier;
  }

  @Override
  public SchemaRegistryClient getSchemaRegistryClient() {
    return srClient;
  }

  @Override
  public Supplier<SchemaRegistryClient> getSchemaRegistryClientFactory() {
    return srClientFactory;
  }

  @Override
  public ConnectClient getConnectClient() {
    return connectClient;
  }

  @Override
  public SimpleKaypherClient getKaypherClient() {
    return kaypherClient;
  }

  @Override
  public void close() {
    adminClient.close();
  }
}
